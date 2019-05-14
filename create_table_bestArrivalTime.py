import boto3


client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.create_table(
        TableName="BestArrivalTime",

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
            "WriteCapacityUnits": 5
        }
    )
    client.get_waiter('table_exists').wait(TableName='BestArrivalTime')
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)

