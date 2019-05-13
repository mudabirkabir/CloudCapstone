import time

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Top10Airports')

task1_queries = [("CMI","ORD"), ("IND","CMH"), ("DFW","IAH"), ("LAX","SFO"), 
                ("JFK","LAX), ("ATL","PHX")]

for combo in task1_queries:
    resp = table.query(
        # Add the name of the index you want to use in your query.
        IndexName="AirportsByDepDelay",
        KeyConditionExpression=Key('Origin').eq("(u\'\"%s\",u\"%s\"\')" % (combo[0],combo[1])),
    )

    print("Mean average delay for source-destination %s is :" % combo)
    for item in resp['Items']:
        print(item)
