import time

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Top10Carriers')

task1_queries = ['CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO']

resp = table.query(
    # Add the name of the index you want to use in your query.
    IndexName="CarriersByDepDelay",
    KeyConditionExpression=Key('Origin').eq('Suspense'),
)

print("The query returned the following items:")
for item in resp['Items']:
    print(item)
