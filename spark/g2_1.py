import os
from pyspark import SparkConf, SparkContext
import boto3
import decimal

s3Bucket = 'mudabircapstone'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('Top10Carriers')

def getFileNames():
    
    s3 = boto3.client('s3')
    keys = []
    resp = s3.list_objects_v2(Bucket=s3Bucket)
    for obj in resp['Contents']:
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
        float(row[27])
        return True
    except:
        print("Value of DepDelay is %s" % (row[27]))
        return False

def saveToDynamodb(result):

    data = result.collect()
    with table.batch_writer() as batch:
        for items in data:
            for item in items[1]:
                batch.put_item(
                    Item={
                        'Origin': items[0],
                        'Carrier': item[0],
                        'DepDelay': decimal.Decimal(str(item[1]))
                    }
                )



conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()
rdd = sc.textFile(','.join(allFiles))

# Filter for all non cancelled flights and map ((airport,carrier), (DepDelay,1))
airportDepDelay = rdd.map(lambda line: line.split(',')) \
                  .filter(notCancelled) \
                  .filter(isFloat) \
                  .map(lambda row: ((row[11],row[6]),(float(row[27]),1)))

#get ((airport,carrier), (totalDepDelay, totalCount))
totalDepDelay = airportDepDelay.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

#get ((airport,carrier), AvgDepDelay)
avgDepDelay = totalDepDelay.mapValues(lambda x: x[0]/x[1])

#for each airport, filter 10 best carriers in terms of Dep delay
result = avgDepDelay.map(lambda (k,v): (k[0],[k[1],v])) \
                    .groupByKey()\
                    .map(lambda (k,v): (k, sorted(v,key=lambda x: x[1], reverse = False))).map(lambda (k,v): (k, v[:10]))

#Save the data to dynamoDB
saveToDynamodb(result)

sc.stop()





