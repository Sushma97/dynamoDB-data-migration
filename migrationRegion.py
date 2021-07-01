#!/usr/bin/env python
from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
import sys
import properties as prp
import concurrent.futures




print("Beginning the data migration for dynamoDB")

src_region = prp.src_region
dest_region = prp.dest_region

ddbc_src = DynamoDBConnection(region=src_region, host='dynamodb.%s.amazonaws.com' % src_region)
ddbc_dest = DynamoDBConnection(region=dest_region, host='dynamodb.%s.amazonaws.com' % dest_region)



def data_migration(ddbc, ddbc_dest, table):

    src_table = table['src_table']  # sys.argv[1]
    dst_table = table['dst_table']
    print("Trasferring data from " + src_table + "in " + src_region + "to " + dst_table + "in " + dest_region)
    # 1. Read and copy the target table to be copied
    try:
        logs = Table(src_table, connection=ddbc)
    except JSONResponseError:
        print("Table %s does not exist" % src_table)
        sys.exit(1)

    print("*** Reading key schema from %s table" % src_table)
    conditionKey = table['key'] + '__in'
    conditionJson = { conditionKey : table['value']}
    print(conditionJson)
    response = logs.scan(**conditionJson)



    # 2. Destination table
    try:
        print("*** Reading key schema from %s table" % dst_table)
        new_logs = Table(dst_table, connection=ddbc_dest)
    except JSONResponseError:
        print("Table %s does not exist" % dst_table)
        sys.exit(1)

    # 3. Add the items
    with new_logs.batch_write() as batch:
        print("Writing the data to " + dst_table)
        for item in response:
            try:
                batch.put_item(item, overwrite=True)
            except ValidationException:
                print(dst_table, item)
            except JSONResponseError:
                print(ddbc_dest.describe_table(dst_table)['Table']['TableStatus'])


with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for table in prp.tables:
        futures.append(executor.submit(data_migration, ddbc_src, ddbc_dest, table))
    for future in concurrent.futures.as_completed(futures):
        print("Done with Copying")
        print(future.result())

print("We are done. Exiting...")