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
        print("Value of row[38] is %s" % (row[38]))
        return False

conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()


rdd = sc.textFile(','.join(allFiles))

# Filter for all non cancelled flights and map (flightID, (DepDelay,1))
flightsDelay = rdd.map(lambda line: line.split(',')) \
               .filter(notCancelled) \
               .filter(isFloat) \
               .map(lambda row: (row[6],(float(row[38]),1)))

# Get (flightID, (totalDelay,totalCount))
totalDelay = flightsDelay.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

# Get (flightID, AvgDelay)
avgDelay = totalDelay.mapValues(lambda x: [x[0],x[1],x[0]/x[1]])

#Filter top 10
result = avgDelay.takeOrdered(10,key=lambda x:x[1][2])

for pair in result:
    print pair[0], pair[1]


sc.stop()
                    
