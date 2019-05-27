import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient

s3Bucket = 'mudabircapstonesample3'

def notCancelled(row):
    try:
        if (float(row[43]) == 0):
            return True
        else:
            return False
    except:
        return False

def printResults(rdd):
    print "-----------------*******----------------------"
    for line in rdd.take(10):
        print line

def streamOut(items):
    kafka = KafkaClient('172.31.40.107:9092,172.31.44.173:9092,172.31.34.192:9092')
    producer = SimpleProducer(kafka, async=False)
    for item in items:
        producer.send_messages('test', item.encode())

sc = SparkContext(appName="streamer")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)

lines = ssc.textFileStream("hdfs:///user/root/input")

rows = lines.map(lambda line: line.split(',')).filter(notCancelled)

reqInfo = rows.map(lambda row: "|".join((row[4],row[6],row[10],row[11], row[18], row[25], row[27], row[38])))

reqInfo.foreachRDD(lambda rdd: printResults(rdd))

reqInfo.foreachRDD(lambda rdd: rdd.foreachPartition(streamOut))

ssc.start()
ssc.awaitTermination()
