import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,OffsetRange,TopicAndPartition
from boto import dynamodb2
from boto.dynamodb2.table import Table,Item
#import boto3
import decimal
import datetime
from time import sleep

dynamoDB = dynamodb2.connect_to_region('us-east-1')
dyntable = Table('BestArrivalTimeFinal2', connection = dynamoDB)

def updateFunction(newValues, minimum):
    if minimum is None:
        minimum =  newValues[0]
    newValues.append(minimum)
    minimum = min(newValues,key=lambda x: x[1])
    return minimum

def printResult(rdd):
    result = rdd.take(10)#Ordered(10,key=lambda x:-x[1])
    print("*******")
    for airport in result:
        print(airport)

def saveToDynamodb(rdd):

    data = rdd.collect()

    for item in data:
        print("*****---****")
        print(item)
        xyz = str(item[0][1]) + '-' + str(item[0][2]) + '-' + str(item[0][3])
        entry = Item(dyntable, data={
                 'XYZ': xyz,
                 'StartDate': str(item[0][0]),
                 'info' : str(item[1][0]),
                 'ArrDelay' : decimal.Decimal(str(item[1][1])) 
            }
        )
        entry.save(overwrite=True)


def isFloat(row):
    try:
        float(row[10])
        float(row[8])
        return True
    except:
        return False

def extractInfo(flight,pm=False):
    flightDate= datetime.date(int(flight[0]), int(flight[1]),int(flight[2]))
    yDest = flight[7]
    if pm:
        yDest = flight[6]
        flightDate -= datetime.timedelta(days=2)
    return ((str(flightDate),yDest),(flight[6], flight[7] , flight[4],flight[5], flight[8],float(flight[10].strip('\"'))))

sc = SparkContext(appName="bestFlights")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
ssc.checkpoint("s3://mudabircapstonecheckpoint/bestFlights/")
topicPartition = TopicAndPartition("airportsAll2", 0)
fromOffset = {topicPartition: 0}
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092"}


stream = KafkaUtils.createDirectStream(ssc, ['airportsAll2'], kafkaParams, fromOffsets = fromOffset)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

runningFlights = rdd.map(lambda line: line.split('|')).filter(isFloat)

XY = [("CMI","ORD"),("JAX","DFW"),("SLC","BFL"),("LAX","SFO"),("DFW","ORD"),("LAX","ORD")]
YZ = [("ORD","LAX"),("DFW","CRP"),("BFL","LAX"),("SFO","PHX"),("ORD","DFW"),("ORD","JFK")]

flightXY = runningFlights.filter(lambda x: float(x[8]) < 1200).map(extractInfo).filter(lambda x: (x[1][0],x[1][1]) in XY)

flightYZ = runningFlights.filter(lambda x: float(x[8]) > 1200).map(lambda flight: extractInfo(flight,True)).filter(lambda x: (x[1][0],x[1][1]) in YZ)

flightXYZ = flightXY.join(flightYZ)

route = flightXYZ.map(lambda (x,y): ((str(x[0]),str(y[0][0]),str(x[1]),str(y[1][1])),(y,y[0][5]+y[1][5])))

totalArrDelay = route.updateStateByKey(updateFunction)

filterkeys = [("2008-03-04","CMI","ORD","LAX"),
              ("2008-09-09","JAX","DFW","CRP"),
              ("2008-04-01","SLC","BFL","LAX"),
              ("2008-07-12","LAX","SFO","PHX"),
              ("2008-06-10","DFW","ORD","DFW"),
              ("2008-01-01","LAX","ORD","JFK")]

result = totalArrDelay.filter(lambda x: x[0] in filterkeys)
#result.foreachRDD(lambda rdd: printResult(rdd))
result.foreachRDD(lambda rdd: saveToDynamodb(rdd))


ssc.start()
ssc.awaitTermination()
                    
