#!/usr/bin/env python

# DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
#
# Created by Sushma Mahadevaswamy on  01/07/2021.
#
# Data deletion script for dynamoDB

from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
import sys
import properties as prp
import concurrent.futures




print("Beginning the data deletion for dynamoDB")

src_region = prp.src_region


ddbc_src = DynamoDBConnection(region=src_region, host='dynamodb.%s.amazonaws.com' % src_region)




def data_deletion(ddbc, table):

    src_table = table['src_table']  # sys.argv[1]
    print("Deleting data from " + src_table + "in " + src_region)
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


    # 2. Delete the items
    with logs.batch_write() as batch:
        print("Deleting the data")
        for item in response:
            try:
                item.delete()
            except ValidationException:
                print(src_table, item)
            except JSONResponseError:
                print(ddbc.describe_table(src_table)['Table']['TableStatus'])


with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for table in prp.tables:
        futures.append(executor.submit(data_deletion, ddbc_src, table))
    for future in concurrent.futures.as_completed(futures):
        print("Done with Deleting")
        print(future.result())

print("We are done. Exiting...")