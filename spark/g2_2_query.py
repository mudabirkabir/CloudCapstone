import time

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Top10Airports')

task1_queries = ["CMI", "BWI", "MIA", "LAX", "IAH", "SFO"]

for origin in task1_queries:
    resp = table.query(
        # Add the name of the index you want to use in your query.
        IndexName="AirportsByDepDelay",
        KeyConditionExpression=Key('Origin').eq("\"%s\"" % origin),
    )

    print("Top 10 airports from Airport %s with best departure delays are:" % origin)
    for item in resp['Items']:
        print(item)
