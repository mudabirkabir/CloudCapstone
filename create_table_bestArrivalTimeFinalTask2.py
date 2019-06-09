import boto3


client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.create_table(
        TableName="BestArrivalTimeFinalTask2",

        KeySchema=[
            {
                "AttributeName": "XYZ",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "StartDate",
                "KeyType": "RANGE"
            }
        ],

        AttributeDefinitions=[
            {
                "AttributeName": "XYZ",
                "AttributeType": "S"
            },
            {
                "AttributeName": "StartDate",
                "AttributeType": "S"
            }       
        ],

        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 2000
        }
    )
    client.get_waiter('table_exists').wait(TableName='BestArrivalTimeFinalTask2')
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)

