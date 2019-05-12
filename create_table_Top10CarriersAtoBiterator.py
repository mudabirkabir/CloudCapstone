import boto3


client = boto3.client('dynamodb', region_name='us-east-1')

try:
    resp = client.create_table(
        TableName="Top10CarriersAtoBiterator",

        KeySchema=[
            {
                "AttributeName": "AtoB",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "Carrier",
                "KeyType": "RANGE"
            }
        ],

        AttributeDefinitions=[
            {
                "AttributeName": "AtoB",
                "AttributeType": "S"
            },
            {
                "AttributeName": "Carrier",
                "AttributeType": "S"
            },
            {
                "AttributeName": "ArrDelay",
                "AttributeType": "N"
            }           
        ],

        LocalSecondaryIndexes=[
            {

                "IndexName": "CarriersByArrDelay",

                "KeySchema": [
                    {
                        "AttributeName": "AtoB",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "ArrDelay",
                        "KeyType": "RANGE"
                    }
                ],

                "Projection": {
                    "ProjectionType": "ALL"
                }
                
            }
        ],

        ProvisionedThroughput={
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
        }
    )
    client.get_waiter('table_exists').wait(TableName='Top10CarriersAtoBiterator')
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)


# Creating a Global seconday index with DepDelay as the primary key

# try:
#     resp = client.update_table(
#         TableName="Top10Airports",

#         AttributeDefinitions=[
#             {
#                 "AttributeName": "DepDelay",
#                 "AttributeType": "N"
#             },
#         ],
#         # This is where we add, update, or delete any global secondary indexes on our table.
#         GlobalSecondaryIndexUpdates=[
#             {
#                 "Create": {
#                     # You need to name your index and specifically refer to it when using it for queries.
#                     "IndexName": "AirportsByDepDelay",
#                     # Like the table itself, you need to specify the key schema for an index.
#                     # For a global secondary index, you can do a simple or composite key schema.
#                     "KeySchema": [
#                         {
#                             "AttributeName": "DepDelay",
#                             "KeyType": "HASH"
#                         }
#                     ],
#                     # You can choose to copy only specific attributes from the original item into the index.
#                     # You might want to copy only a few attributes to save space.
#                     "Projection": {
#                         "ProjectionType": "ALL"
#                     },
#                     # Global secondary indexes have read and write capacity separate from the underlying table.
#                     "ProvisionedThroughput": {
#                         "ReadCapacityUnits": 10,
#                         "WriteCapacityUnits": 10,
#                     }
#                 }
#             }
#         ],
#     )
#     print("Secondary index added!")
# except Exception as e:
#     print("Error updating table:")
#     print(e)


