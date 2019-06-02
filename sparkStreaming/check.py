import os
from pyspark import SparkConf, SparkContext
import boto3

def notCancelled(row):
    try:
        if (float(row[43]) == 0):
            return True
        else:
            return False
    except:
        return False

def writeToFile(rdd,f):
    for line in rdd.collect():
        f.write(line+"\n")

s3Bucket = 'mudabircapstone'
def getFileNames():
    
    s3 = boto3.client('s3')
    keys = []
    resp = s3.list_objects_v2(Bucket=s3Bucket)
    for obj in resp['Contents']:
        keys.append('s3://%s/%s' %(s3Bucket,obj['Key']))
    
    return keys

# def streamOut(items):
#     producer = KafkaProducer(bootstrap_servers=['172.31.40.107:9092','172.31.44.173:9092','172.31.34.192:9092'])
#     for item in items:
#         producer.send('test', item)

conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()

lines = sc.textFile(','.join(allFiles))

rows = lines.map(lambda line: line.replace('"', '')).map(lambda line: line.split(',')).filter(notCancelled)

reqInfo = rows.map(lambda row: "|".join((row[0],row[2],row[3],row[4],row[6],row[10],row[11], row[18], row[25], row[27], row[38])))

count = reqInfo.count()

print("**********************")
print("Count value is %s" % str(count))
print("**********************")
sc.stop()