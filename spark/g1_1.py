import os
from pyspark import SparkConf, SparkContext
import boto3


def getFileNames():
	
	s3 = boto3.client('s3')
	keys = []
	resp = s3.list.objects_v2(Bucket='mudabircapstone')
	for obj in resp['Contents']:
		keys.append(obj['Key'])
	
	return keys




conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()

#debugprint
for inputFile in allFiles:
	print(inputFile)

#rdd = sc.textFile(inputFile)

sc.stop()