import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def printResult(rdd:
    result = rdd.takeOrdered(10,key=lambda x:-x[1])
    for airport in result:
        print(airport)

sc = SparkContext(appName="top10airports")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)

kafkaParams = {"metadata.broker.list": "172.31.40.107:9092,172.31.44.173:9092,172.31.34.192:9092",
               "auto.offset.reset": "smallest"}


stream = KafkaUtils.createDirectStream(ssc, ['airports'], kafkaParams)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

airports = rdd.map(lambda line: line.split('|')).flatMap(lambda row: [row[6],row[7]])

counts = airports.map(lambda x: (x,1)).updateStateByKey(updateFunction)

counts.foreachRDD(lamdba rdd: printResult(rdd))


scc.start()
ssc.awaitTermination()
                    
