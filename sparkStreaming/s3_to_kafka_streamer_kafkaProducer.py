import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka import KafkaProducer


def notCancelled(row):
    try:
        if (float(row[43]) == 0):
            return True
        else:
            return False
    except:
        return False

def writeToFile(rdd,f):
    for line in rdd.collect():
        f.write(line+"\n")

# def streamOut(items):
#     producer = KafkaProducer(bootstrap_servers=['172.31.40.107:9092','172.31.44.173:9092','172.31.34.192:9092'])
#     for item in items:
#         producer.send('test', item)

sc = SparkContext(appName="streamer")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)

lines = ssc.textFileStream("hdfs:///user/root/input")

rows = lines..map(lambda line: line.replace('"', '')).map(lambda line: line.split(',')).filter(notCancelled)

reqInfo = rows.map(lambda row: "|".join((row[0],row[2],row[3],row[4],row[6],row[10],row[11], row[18], row[25], row[27], row[38])))

f = open("filteredData","w+")
reqInfo.foreachRDD(lambda rdd: writeToFile(rdd))

#reqInfo.foreachRDD(lambda rdd: rdd.foreachPartition(streamOut))

ssc.start()
ssc.awaitTermination()
