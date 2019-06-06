import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
import boto3
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('Top10Airports2')

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = (0, 0, 0)
    depDelaySum = sum(newValues[0], runningCount[0])
    count    = runningCount[1] + newValues[1]
    avgDepDelay = depDelaySum/count
    return avgDepDelay

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

    data = result.collect()
    with table.batch_writer() as batch:
        for items in data:
            for item in items[1]:
                batch.put_item(
                    Item={
                        'Origin': items[0],
                        'Dest': item[0],
                        'DepDelay': decimal.Decimal(str(item[1]))
                    }
                )


def isFloat(row):
    try:
        float(row[9])
        return True
    except:
        return False

sc = SparkContext(appName="top10airportsByairports")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
topicPartition = TopicAndPartition("airportsFull", 0)
fromOffset = {topicPartition: 0}
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092"}


stream = KafkaUtils.createDirectStream(ssc, ['airportsFull'], kafkaParams, fromOffsets = fromOffset)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

flightsDelay = rdd.map(lambda line: line.split('|')).filter(isFloat).map(lambda row: ((row[6],row[7]),(float(row[9]),1)))

avgDepDelay = flightsDelay.updateStateByKey(updateFunction)

avgDepDelay = avgDepDelay.map(lambda row: (row[0][0], (row[0][1],row[1])))

result = avgDepDelay.transform(lambda rdd: rdd.aggregateByKey([],sortLocal,sortAll))

result.foreachRDD(saveToDynamodb)


sSc.start()
ssc.awaitTermination()
                    
