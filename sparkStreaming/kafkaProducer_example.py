from kafka import SimpleProducer, KafkaClient

# To send messages synchronously
kafka = KafkaClient('172.31.40.107:9092,172.31.44.173:9092,172.31.34.192:9092')
producer = SimpleProducer(kafka)

# Note that the application is responsible for encoding messages to type bytes
producer.send_messages('test', 'some message'.encode())
producer.send_messages('test', 'this')
