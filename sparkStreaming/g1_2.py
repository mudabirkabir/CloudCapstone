import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = (0, 0, 0)
    delaySum = sum(newValues[0], runningCount[0])
    count    = runningCount[1] + newValues[1]
    avgArrivalDelay = delaySum/count
    return (delaySum,count,avgArrivalDelay)

def printResult(rdd):
    result = rdd.take(10)#Ordered(10,key=lambda x:-x[1])
    for airport in result:
        print(airport)

def isFloat(row):
    try:
        float(row[10])
        return True
    except:
        return False

sc = SparkContext(appName="top10airports")
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

flightsDelay = rdd.map(lambda line: line.split('|')).filter(isFloat).map(lambda row: (row[4],(float(row[10]),1)))

counts = flightsDelay.updateStateByKey(updateFunction)

sorted_counts = counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1][2]))

sorted_counts.foreachRDD(lambda rdd: printResult(rdd))


ssc.start()
ssc.awaitTermination()
                    
