import os
from pyspark import SparkConf, SparkContext
import boto3

s3Bucket = 'mudabircapstone'
def getFileNames():
	
	s3 = boto3.client('s3')
	keys = []
	resp = s3.list_objects_v2(Bucket=s3Bucket)
	for obj in resp['Contents']:
		keys.append('s3://%s/%s' %(s3Bucket,obj['Key']))
	
	return keys


conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()

rdd = sc.textFile(','.join(allFiles))

airports = rdd.map(lambda line: line.split(',')).flatMap(lambda row: [row[11],row[18]])

counts = airports.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

result = counts.takeOrdered(10,key=lambda x:-x[1])

for pair in result:
	print(pair)


sc.stop()
                    
