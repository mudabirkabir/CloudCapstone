import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
import boto3
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('MeanDelayBetweenAandB2')

def updateFunction(newValue, minimum):
    if minimum is None:
        return newValue
    if newValue[1] < minimum[1]:
        minimum = newValue
    return minimum

def printResult(rdd):
    result = rdd.take(10)#Ordered(10,key=lambda x:-x[1])
    for airport in result:
        print(airport)

def sortLocal(top10, newVal):
    top10.append(newVal)
    top10.sort(key=lambda element: element[1])
    return top10[0:10]

def merge(list1, list2):
    for x in list2:
        list1.append(list2)
    list1.sort(key=lambda element: element[1])
    return list1[0:10]

def saveToDynamodb(result):

    with table.batch_writer() as batch:
        for item in result:
            batch.put_item(
                Item={
                    'XYZ': str(item[0][1]+ '-' + item[0][2] + '-' + item[0][3]),
                    'StartDate': str(item[0][0]),
                    'info' : str(item[1][0]),
                    'ArrDelay' : decimal.Decimal(str(item[1][1])) 
                }
            )


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

sc = SparkContext(appName="airportsToAirportsDelay")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
topicPartition = TopicAndPartition("airportsAll2", 0)
fromOffset = {topicPartition: 0}
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092"}


stream = KafkaUtils.createDirectStream(ssc, ['airportsAll2'], kafkaParams, fromOffsets = fromOffset)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

runningFlights = rdd.map(lambda line: line.split('|')).filter(isFloat)

flightXY = runningFlights.filter(lambda x: float(x[8]) < 1200).map(extractInfo)

flightYZ = runningFlights.filter(lambda x: float(x[8]) > 1200).map(lambda flight: extractInfo(flight,True))

flightXYZ = flightXY.join(flightYZ)

route = flightXYZ.map(lambda (x,y): ((x[0],y[0][0].encode('ascii','ignore'),x[1].encode('ascii','ignore'),y[1][1].encode('ascii','ignore')),(y,y[0][5]+y[1][5])))

totalArrDelay = route.updateStateByKey(updateFunction)


totalArrDelay.foreachRDD(saveToDynamodb)


sSc.start()
ssc.awaitTermination()
                    
