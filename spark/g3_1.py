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

# #debugprint
# for inputFile in allFiles:
#   print(inputFile)

#rdd = sc.textFile('s3://%s//*' % s3Bucket)
#rdd = sc.textFile('s3://%s/Sample.csv' % s3Bucket)
rdd = sc.textFile(','.join(allFiles))

airports = rdd.map(lambda line: line.split(',')).flatMap(lambda row: [row[11],row[18]])

counts = airports.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

#result = counts.map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda y: (y[1],y[0]))

#result = counts.sortBy(lambda x: x[1], ascending=False).takeOrdered(10,key=lambda x:-x[1])
result = counts.sortBy(lambda x: x[1], ascending=False).collect()

#result.saveAsTextFile("/home/hadoop/CloudCapstone/spark/g3_1.log")
f = open("g3_1.log","w+")
for i in result:
  f.write(str(i)+"\n")

f.close()
sc.stop()
                    