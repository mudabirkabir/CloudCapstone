import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext



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

rows = lines.map(lambda line: line.replace('"', '')).map(lambda line: line.split(','))

reqInfo = rows.map(lambda row: "|".join((row[0],row[2],row[3],row[4],row[6],row[10],row[11], row[18], row[25], row[27], row[38], row[43])))

f = open("/home/hadoop/output/filteredDataIncludingCancelled","w+")
reqInfo.foreachRDD(lambda rdd: writeToFile(rdd,f))

#reqInfo.foreachRDD(lambda rdd: rdd.foreachPartition(streamOut))

ssc.start()
ssc.awaitTermination()
f.close()
