import os
from pyspark import SparkConf, SparkContext
import boto3
import decimal
import datetime
from time import sleep

s3Bucket = 'mudabircapstone'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('BestArrivalTimeFasr')

def getFileNames():
    
    s3 = boto3.client('s3')
    keys = []
    resp = s3.list_objects_v2(Bucket=s3Bucket)
    for obj in resp['Contents']:
        if '2008' in obj['Key']:
            keys.append('s3://%s/%s' %(s3Bucket,obj['Key']))
    
    return keys


def notCancelled(row):
    try:
        if (float(row[43]) == 0):
            return True
        else:
            return False
    except:
        return False

def isFloat(row):
    try:
        float(row[25].strip('\"'))
        float(row[38].strip('\"'))
        return True
    except:
        print("Value of CRSDepTime is %s" % (row[25]))
        return False

def saveToDynamodb(result):

    data = result.toLocalIterator()
    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(
                Item={
                    'DateXYZ': str(item[0]),
                    'info' : str(item[1][0]),
                    'ArrDelay' : decimal.Decimal(str(item[1][1])) 
                }
            )

def extractInfo(flight,pm=False):
    flightDate= datetime.date(int(flight[0]), int(flight[2]),int(flight[3]))
    yDest = flight[18]
    if pm:
        yDest = flight[11]
        flightDate -= datetime.timedelta(days=2)
    return ((str(flightDate),yDest),(flight[11], flight[18] , flight[6],flight[10], flight[25],float(flight[38].strip('\"'))))

# Origin = 11
# Dest  = 18
#Airline = 6
# Flight Number = 10
# CRSDepTime = 25
# ArrDelay = 38
# year = 0
# Month = 2
# DayofMonth = 3
conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()
rdd = sc.textFile(','.join(allFiles))

runningFlights = rdd.map(lambda line: line.split(',')) \
                  .filter(notCancelled) \
                  .filter(isFloat)

flightXY = runningFlights.filter(lambda x: float(x[25].strip('\"')) < 1200).map(extractInfo)

flightYZ = runningFlights.filter(lambda x: float(x[25].strip('\"')) > 1200).map(lambda flight: extractInfo(flight,True))

flightXYZ = flightXY.join(flightYZ)

route = flightXYZ.map(lambda (x,y): ((x[0],y[0][0],x[1],y[1][1]),(y,y[0][5]+y[1][5])))

totalArrDelay = route.reduceByKey(lambda y1,y2: y1 if y1[1] < y2[1] else y2)

#print("====++Total number of partitions++==== : %s" % str(totalArrDelay.getNumPartitions()))

saveToDynamodb(totalArrDelay)
#totalArrDelay.repartition(200)
#print("====++After reparitioning++==== : %s" % str(totalArrDelay.getNumPartitions()))

#sample = totalArrDelay.take(10)

# print("====Received 10 samples =====")
# for data in sample:
#     print(data)
sleep(30)
sc.stop()





