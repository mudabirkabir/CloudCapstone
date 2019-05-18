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

#Same as g1_1.py until this point
result = counts.sortBy(lambda x: x[1], ascending=False).collect()

#Write the results to a text file for plotting
f = open("g3_1.log","w+")
for i in result:
  f.write(str(i)+"\n")

f.close()
sc.stop()
                    
