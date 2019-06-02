import os
from kafka import SimpleProducer, KafkaClient

# To send messages synchronously
kafka = KafkaClient('172.31.40.107:9092,172.31.44.173:9092,172.31.34.192:9092')
producer = SimpleProducer(kafka)


file = open('/home/hadoop/output/filteredData', 'r')

for line in file:
    producer.send_messages('airports', line)

file.close()