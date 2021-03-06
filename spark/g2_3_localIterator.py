import os
from pyspark import SparkConf, SparkContext
import boto3
import decimal

s3Bucket = 'mudabircapstone'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('Top10CarriersAtoBiterator')

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
        float(row[38])
        return True
    except:
        print("Value of ArrivalDelay is %s" % (row[38]))
        #sys.exit("Value of row[39] is %s" % (row[39]))
        return False

def saveToDynamodb(result):

    data = result.toLocalIterator()
    with table.batch_writer() as batch:
        for items in data:
            for item in items[1]:
                batch.put_item(
                    Item={
                        'AtoB': str(items[0]),
                        'Carrier': item[0],
                        'ArrDelay': decimal.Decimal(str(item[1]))
                    }
                )



conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()
rdd = sc.textFile(','.join(allFiles))

airportArrDelay = rdd.map(lambda line: line.split(',')) \
                  .filter(notCancelled) \
                  .filter(isFloat) \
                  .map(lambda row: ((row[11],row[18], row[6]),(float(row[38]),1)))

totalArrDelay = airportArrDelay.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

avgArrDelay = totalArrDelay.mapValues(lambda x: x[0]/x[1])

result = avgArrDelay.map(lambda (k,v): (k[:2],[k[2],v])) \
                    .groupByKey()\
                    .map(lambda (k,v): (k, sorted(v,key=lambda x: x[1], reverse = False))).map(lambda (k,v): (k, v[:10]))


# data = result.collect()
# for items in data:
#     for item in items[1]:
#         print(items[0])
#         print(item[0])
#         print(item[1])
#         break
saveToDynamodb(result)

sc.stop()





