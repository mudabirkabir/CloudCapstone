from boto import dynamodb2
from boto.dynamodb2.table import Table,Item
import decimal


dynamoDB = dynamodb2.connect_to_region('us-east-1')
dyntable = Table('Top10Carriers2', connection = dynamoDB)

entries = dyntable.query_2(Origin__eq="CMI")

for entry in entries:
    #dyntable.delete_item(Origin="IAH",Carrier=entry['Carrier'])
    print(entry['Carrier'])

