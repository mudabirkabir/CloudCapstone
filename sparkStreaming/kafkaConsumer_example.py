from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,OffsetRange, TopicAndPartition

def printBatch(rdd):
    print "-----------------*******----------------------"
    for line in rdd.collect():
        print line

sc = SparkContext(appName="streamer")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)

#topic = self._randomTopic()
#sendData = {"a": 1, "b": 2, "c": 3}
topicPartition = TopicAndPartition("testing",0)
fromOffset = {topicPartition: 0}
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092",
               "auto.offset.reset": "smallest"}

#self._kafkaTestUtils.createTopic(topic)
#self._kafkaTestUtils.sendMessages(topic, sendData)

stream = KafkaUtils.createDirectStream(ssc, ['testing'], kafkaParams, fromOffsets = fromOffset)

check = stream.transform(lambda rdd: rdd.sortBy(lambda x: x[1]))

check.foreachRDD(lambda rdd: printBatch(rdd))
ssc.start()
ssc.awaitTermination()
