import os
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,OffsetRange,TopicAndPartition
#import boto3
from boto import dynamodb2
from boto.dynamodb2.table import Table,Item
import decimal

#dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
#table = dynamodb.Table('Top10Carriers2')
dynamoDB = dynamodb2.connect_to_region('us-east-1')
dyntable = Table('Top10Carriers2', connection = dynamoDB)


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = (0, 0, 0)
    depDelaySum = sum(newValues, runningCount[0])
    count    = runningCount[1] + len(newValues)
    avgDepDelay = depDelaySum/float(count)
    return (depDelaySum,count, avgDepDelay)

def printResult(rdd):
    result = rdd.take(10)#Ordered(10,key=lambda x:-x[1])
    for airport in result:
        print(airport)

def sortLocal(top10, newVal):
    top10.append(newVal)
    top10.sort(key=lambda element: element[1])
    return top10[0:10]

def merge(list1, list2):
    for x in list2:
        list1.append(x)
    list1.sort(key=lambda element: element[1])
    return list1[0:10]

def saveToDynamodb(rdd):

    data = rdd.collect()
    '''with table.batch_writer() as batch:
        for items in data:
            for item in items[1]:
                batch.put_item(
                    Item={
                        'Origin': items[0],
                        'Carrier': item[0],
                        'DepDelay': decimal.Decimal(str(item[1]))
                    }
                )'''
    for items in data:
        for item in items[1]:
            entry = Item(dyntable, data={
                    'Origin': items[0],
                    'Carrier': item[0],
                    'DepDelay': decimal.Decimal(str(item[1]))
                }
            )
            entry.save(overwrite=True)


def isFloat(row):
    try:
        float(row[9])
        return True
    except:
        return False

sc = SparkContext(appName="top10carriersByairports")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
ssc.checkpoint("s3://mudabircapstonecheckpoint/top10carriersByairport/")
topicPartition = TopicAndPartition("airportsFull", 0)
fromOffset = {topicPartition: 0}
kafkaParams = {"metadata.broker.list": "b-2.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092,b-1.kafkacluster.qa2zr3.c2.kafka.us-east-1.amazonaws.com:9092"}


stream = KafkaUtils.createDirectStream(ssc, ['airportsFull'], kafkaParams, fromOffsets = fromOffset)

'''
The incoming data format is
Year|Month|date|DayofWeek|UniqueCarrier|FlightNum|Origin|Dest|CRSDeptime|DepDelay|ArrDelay
'''

rdd = stream.map(lambda x: x[1])

flightsDelay = rdd.map(lambda line: line.split('|')).filter(isFloat).map(lambda row: ((row[6],row[4]),float(row[9])))

avgDepDelay = flightsDelay.updateStateByKey(updateFunction)

avgDepDelay = avgDepDelay.map(lambda row: (row[0][0], (row[0][1],row[1][2])))

result2 = avgDepDelay.transform(lambda rdd: rdd.aggregateByKey([],sortLocal,merge))

result2 = result2.filter(lambda x: x[0] in ['CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO'])


result2.foreachRDD(lambda rdd: printResult(rdd))
#result2.foreachRDD(lambda rdd: saveToDynamodb(rdd))

ssc.start()
ssc.awaitTermination()
                    
