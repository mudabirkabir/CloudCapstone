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

flightsDelay = rdd.map(lambda line: line.split(','))/
               .filter(lambda row: int(row[43])== 0) /
               .map(lambda row: (row[4],(int(row[39]),1)))

totalDelay = flightsDelay.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

avgDelay = totalDelay.mapValues(lambda x: x[0]/x[1])

#result = counts.map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda y: (y[1],y[0]))

#result = counts.sortBy(lambda x: x[1], ascending=False).takeOrdered(10,key=lambda x:-x[1])
result = avgDelay.takeOrdered(10,key=lambda x:-x[1])

for pair in result:
    print(pair)

#Check what is parallelize
# >>> sc.parallelize(tmp).sortBy(lambda x: x[0]).collect()



sc.stop()
                    
