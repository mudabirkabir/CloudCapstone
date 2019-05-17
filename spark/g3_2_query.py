import time

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('BestArrivalTimeFinal')


#Date in year-mm-day format
task3_2_queries = (
                ['\"CMI\"-\"ORD\"-\"LAX\"', "2008-03-04"],
                ['\"JAX\"-\"DFW\"-\"CRP\"', "2008-09-09"],
                ['\"SLC\"-\"BFL\"-\"LAX\"', "2008-04-01"],
                ['\"DFW\"-\"ORD\"-\"DFW\"', "2008-06-10"],
                ['\"LAX\"-\"ORD\"-\"JFK\"', "2008-01-01"],
                ['\"LAX\"-\"SFO\"-\"PHX\"', "2008-07-12"]
                  )

for route in task3_2_queries:
    resp = table.get_item(
        Key={
        'XYZ': str(route[0]),
        'StartDate': str(route[1])
   	}
    )
    print(route)
    print("Shortest Arrival Delay is %s" % str(resp['Item']['ArrDelay']))
    print("Journey first leg in below order")
    print("origin, dest, flight, flight number, deptime, Arrival delay")
    print(str(resp['Item']['info']).replace('"','').split("),")[0] + '))')
    print("Journey Second leg in below order")
    print("origin, dest, flight, flight number, deptime, Arrival delay")
    print('(' + str(resp['Item']['info']).replace('"','').split("),")[1])
    print("----------------------------------")
