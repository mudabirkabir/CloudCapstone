import os
import time
from kafka import KafkaProducer

# To send messages synchronously

producer = KafkaProducer(bootstrap_servers=['b-2.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092','b-3.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092','b-1.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092'],batch_size=98304,linger_ms=100,acks='all')

file = open('/home/hadoop/output/filteredDataIncludingCancelled', 'r')

for line in file:
    producer.send('airportsWithCancelled', line)

time.sleep(100)
file.close()
