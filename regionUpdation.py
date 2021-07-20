#!/usr/bin/env python

# DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
#
# Created by Sushma Mahadevaswamy on  01/07/2021.
#
# Data migration script for dynamoDB


import boto3
from boto.exception import JSONResponseError
from boto3.dynamodb.conditions import Attr
import properties as prp
import  sys


print("Beginning the region updation for dynamoDB")
def regionUpdation(env,puids,region):


    table_name = prp.cxpp_customer_region_mapping_table.replace('env',env)
    dynamodb = boto3.resource('dynamodb')
    try:
        print("*** Reading key schema from %s table" % table_name)
        table = dynamodb.Table(table_name)
    except JSONResponseError:
        print("Table %s does not exist" % table_name)
        sys.exit(1)

    for puid in puids:
        print("**Updating region for puId :: " + puid)
        scan_kwargs = {
            'FilterExpression': Attr("puId").eq(int(puid))
        }
        response = table.scan(**scan_kwargs)
        print(response['Items'])
        for item in response['Items']:
            print("Updating region for customerId :: " + item['customerId'])
            table.update_item(
                Key={
                    'customerId':  item['customerId'],
                    'puId':int(puid)

                    },
                UpdateExpression='set partnerRegion=:partnerRegion',
                ExpressionAttributeValues={':partnerRegion': region},
                ReturnValues = "UPDATED_NEW"
                        )


def regionAwsTablUpdate(env,puids,region):
    table_name = prp.cxpp_aws_region_mapping_table.replace('env', env)
    dynamodb = boto3.resource('dynamodb')
    try:
        print("*** Reading key schema from %s table" % table_name)
        table = dynamodb.Table(table_name)
    except JSONResponseError:
        print("Table %s does not exist" % table_name)
        sys.exit(1)

    for puid in puids:
        print("\n**Updating region for PUID :: " + puid)
        scan_kwargs = {
            'FilterExpression': Attr("puId").eq(int(puid))
            }
        response = table.scan(**scan_kwargs)
        print(response['Items'])
        for item in response['Items']:
            print("Updating region for partnerId :: " + str(item['partnerId']))
            #First Delete the item
            table.delete_item(
            Key={
              'partnerId':  item['partnerId'],
              'awsRegion': item['awsRegion']
                }
                            )

            #Adding back the item
            item['awsRegion'] = region
            table.put_item(Item = item)


def regionUpdateForUserDetails(env,puids,region):


    table_name = prp.cxpp_user_detail_table.replace('env', env)
    dynamodb = boto3.resource('dynamodb')
    try:
        print("*** Reading key schema from %s table" % table_name)
        table = dynamodb.Table(table_name)
    except JSONResponseError:
        print("Table %s does not exist" % table_name)
        sys.exit(1)

    for puid in puids:
        print("\n**Updating region for PUID :: " + puid)
        scan_kwargs = {
            'FilterExpression': Attr("puId").eq(int(puid))
        }
        response = table.scan(**scan_kwargs)

        for item in response['Items']:
          print("Updating region for userId :: " + item['userId'])
          table.update_item(
                Key={
                    'userId':  item['userId'],
                    'sk':item['sk']

                },
                UpdateExpression='set #rn=:val1',
                ExpressionAttributeNames={
                     "#rn": "region"
              },
                ExpressionAttributeValues={':val1': region},
                ReturnValues = "UPDATED_NEW"
                )

print("We are done. Exiting...")