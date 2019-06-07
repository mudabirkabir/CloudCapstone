import time

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('MeanDelayBetweenAandB2')

task1_queries = [("CMI","ORD"), ("IND","CMH"), ("DFW","IAH"), ("LAX","SFO"), ("JFK","LAX"), ("ATL","PHX")]

for combo in task1_queries:
    resp = table.query(
        KeyConditionExpression=Key('AtoB').eq("(u\'\"%s\"\', u\'\"%s\"\')" % (combo[0],combo[1])),
    )

    print("Mean average delay for source-destination (u\'\"%s\"\', u\'\"%s\"\') is :" % (combo[0],combo[1]))
    for item in resp['Items']:
        print(item)
