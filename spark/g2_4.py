import os
from pyspark import SparkConf, SparkContext
import boto3
import decimal

s3Bucket = 'mudabircapstone'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('MeanDelayBetweenAandB')

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

    data = result.collect()
    with table.batch_writer() as batch:
        for items in data:
            for item in items[0]:
                batch.put_item(
                    Item={
                        'Origin': item[0],
                        'Dest': item[1],
                        'ArrDelay': decimal.Decimal(str(items[1]))
                    }
                )



conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()
rdd = sc.textFile(','.join(allFiles))

airportDepDelay = rdd.map(lambda line: line.split(',')) \
                  .filter(notCancelled) \
                  .filter(isFloat) \
                  .map(lambda row: ((row[11],row[18]),(float(row[38]),1)))

totalDepDelay = airportDepDelay.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

avgDepDelay = totalDepDelay.mapValues(lambda x: x[0]/x[1])

saveToDynamodb(result)

sc.stop()





