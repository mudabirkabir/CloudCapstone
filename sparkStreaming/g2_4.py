import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,OffsetRange,TopicAndPartition
#import boto3
from boto import dynamodb2
from boto.dynamodb2.table import Table,Item
import decimal

#dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
#table = dynamodb.Table('MeanDelayBetweenAandB2')
dynamoDB = dynamodb2.connect_to_region('us-east-1')
dyntable = Table('MeanDelayBetweenAandBTask2', connection = dynamoDB)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = (0, 0, 0)
    ArrDelaySum = sum(newValues, runningCount[0])
    count    = runningCount[1] + len(newValues)
    avgArrDelay = ArrDelaySum/float(count)
    return (ArrDelaySum,count, avgArrDelay)

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
        list1.append(x)
    list1.sort(key=lambda element: element[1])
    return list1[0:10]

def saveToDynamodb(rdd):

    data = rdd.collect()

    for item in data:
        entry = Item(dyntable, data={
                'AtoB': str(item[0]),
                'ArrDelay': decimal.Decimal(str(item[1]))
            }
        )
        entry.save(overwrite=True)

    # with table.batch_writer() as batch:
    #     for item in data:
    #         batch.put_item(
    #             Item={
    #                 'AtoB': str(item[0]),
    #                 'ArrDelay': decimal.Decimal(str(item[1]))
    #             }
    #         )


def isFloat(row):
    try:
        float(row[10])
        return True
    except:
        return False

sc = SparkContext(appName="AtoBdelay")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
ssc.checkpoint("s3://mudabircapstonecheckpoint/meanDelayBetweenAandB/")
topicPartition = TopicAndPartition("airportsFull", 0)
fromOffset = {topicPartition: 0}
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092"}


stream = KafkaUtils.createDirectStream(ssc, ['airportsFull'], kafkaParams, fromOffsets = fromOffset)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

flightsDelay = rdd.map(lambda line: line.split('|')).filter(isFloat).map(lambda row: ((row[6],row[7]),float(row[10])))

avgDepDelay = flightsDelay.updateStateByKey(updateFunction)

result = avgDepDelay.map(lambda row: (row[0],row[1][2]))

result = result.filter(lambda x: x[0] in [('CMI','ORD'),('IND','CMH'),('DFW','IAH'),('LAX','SFO'),('JFK','LAX'),('ATL','PHX'),('LGA','BOS'),('BOS','LGA'),('OKC','DFW'),('MSP','ATL')])

result.foreachRDD(lambda rdd: printResult(rdd))
result.foreachRDD(lambda rdd: saveToDynamodb(rdd))

ssc.start()
ssc.awaitTermination()
                    
