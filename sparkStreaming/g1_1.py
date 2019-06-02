import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def printResult(rdd,f):
    result = rdd.take(10)#Ordered(10,key=lambda x:-x[1])
    for airport in result:
        print(airport)
        f.write(airport+"\n")

sc = SparkContext(appName="top10airports")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
ssc.checkpoint("s3://mudabircapstonecheckpoint/top10airports/")
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092"}


stream = KafkaUtils.createDirectStream(ssc, ['airportsFull'], kafkaParams)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

airports = rdd.map(lambda line: line.split('|')).flatMap(lambda row: [row[6],row[7]])

counts = airports.map(lambda x: (x,1)).updateStateByKey(updateFunction)

sorted_counts = counts.transform(lambda rdd: rdd.sortBy(lambda x: -x[1]))

f =  open("/home/hadoop/CloudCapstone\sparkStreaming\output\g1_1","w+")
counts.foreachRDD(lambda rdd: printResult(rdd,f))


ssc.start()
ssc.awaitTermination()
f.close()                   
