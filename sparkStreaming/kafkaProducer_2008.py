import os
import time
from kafka import KafkaProducer

# To send messages synchronously

producer = KafkaProducer(bootstrap_servers=['b-2.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092','b-1.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092','b-3.kafkacluster.kfbj9j.c2.kafka.us-east-1.amazonaws.com:9092'],batch_size=98304,linger_ms=100,acks='all')

file = open('/home/hadoop/output/filteredData_2008', 'r')

for line in file:
    producer.send('airportsAll2', line)

time.sleep(10)
file.close()
