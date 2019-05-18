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
        print("Value of row[39] is %s" % (row[38]))
        return False


conf = SparkConf()
sc = SparkContext(conf = conf)

allFiles = []
allFiles = getFileNames()


rdd = sc.textFile(','.join(allFiles))

# Filter for all non cancelled flights and map (day, (DepDelay,1))
flightsDelay = rdd.map(lambda line: line.split(',')) \
               .filter(notCancelled) \
               .filter(isFloat) \
               .map(lambda row: (row[4],(float(row[38]),1)))

# Get (day, (totalDelay,totalCount))
totalDelay = flightsDelay.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

# Get (day, AvgDelay)
avgDelay = totalDelay.mapValues(lambda x: [x[0],x[1],x[0]/x[1]])

#Sort the days by best Departure delay
result = avgDelay.takeOrdered(7,key=lambda x:x[1][2])

days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

for pair in result:
    print days[pair[0]], pair[1]

sc.stop()
                    
