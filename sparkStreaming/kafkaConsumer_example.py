from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def printBatch(rdd):
    print "-----------------*******----------------------"
    for line in rdd.collect():
        print line

sc = SparkContext(appName="streamer")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)

#topic = self._randomTopic()
#sendData = {"a": 1, "b": 2, "c": 3}
kafkaParams = {"metadata.broker.list": "172.31.40.107:9092,172.31.44.173:9092,172.31.34.192:9092",
               "auto.offset.reset": "smallest"}

#self._kafkaTestUtils.createTopic(topic)
#self._kafkaTestUtils.sendMessages(topic, sendData)

stream = KafkaUtils.createDirectStream(self.ssc, ['test'], kafkaParams)

stream.foreachRDD(lambda rdd: printBatch(rdd))
self.ssc.start()
ssc.awaitTermination()