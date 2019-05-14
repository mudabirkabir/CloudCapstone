import boto3


client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.create_table(
        TableName="BestArrivalTimeFast",

        KeySchema=[
            {
                "AttributeName": "DateXYZ",
                "KeyType": "HASH"
            }
        ],

        AttributeDefinitions=[
            {
                "AttributeName": "DateXYZ",
                "AttributeType": "S"
            }         
        ],

        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 3600
        }
    )
    client.get_waiter('table_exists').wait(TableName='BestArrivalTimeFast')
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)

#