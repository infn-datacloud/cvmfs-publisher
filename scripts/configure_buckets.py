#!/usr/bin/python3

import boto3
import sys
from rgwadmin import RGWAdmin
import argparse 

PARSER = argparse.ArgumentParser()
PARSER.add_argument('-a', '--access-key', dest='a',
                    help='access key')
PARSER.add_argument('-s', '--secret-key', dest='s',
                    help='secret key')
PARSER.add_argument('-u', '--endpoint', dest='u',
                    help='endpoint rgw')
PARSER.add_argument('-r', '--role', dest='r',
                    help='role')
PARSER.add_argument('-arn', '--topic-arn',dest='arn',
                    help='topic arn')
PARSER.add_argument('-v', '--verbose', action='store_true', dest='v',
                    help='increase verbosity')
ARGS = PARSER.parse_args()
ACCESS_KEY = ARGS.a
SECRET_KEY = ARGS.s
ENDPOINT = ARGS.u
ROLE = ARGS.r
TOPIC_ARN = ARGS.arn

if ARGS.v:
    boto3.set_stream_logger(name='botocore')

rgw = RGWAdmin(access_key=ACCESS_KEY, secret_key=SECRET_KEY, server=ENDPOINT.split('//')[1])
bucket_list = rgw.get_buckets()

sts_client = boto3.client('sts',
    aws_access_key_id =ACCESS_KEY ,
    aws_secret_access_key =SECRET_KEY,
    endpoint_url=ENDPOINT,
    region_name='default',
    )
response = sts_client.assume_role(
    RoleArn=f'arn:aws:iam:::role/{ROLE}',
    RoleSessionName='Bob',
    DurationSeconds=3600
    )

s3_client = boto3.client('s3',
    aws_access_key_id = response['Credentials']['AccessKeyId'],
    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
    aws_session_token = response['Credentials']['SessionToken'],
    endpoint_url=ENDPOINT,
    region_name='default',)

# Check topic and topic_arn
sns_client = boto3.client('sns',
    aws_access_key_id = ACCESS_KEY,
    aws_secret_access_key = SECRET_KEY,
    endpoint_url=ENDPOINT,
    region_name='default',
    )
resp = sns_client.list_topics()
print(resp['Topics'])
for topic in resp['Topics']:
    if topic['TopicArn'] == TOPIC_ARN:
        print('Topic ready for bucket configuration')
        break
    else:
        print('Create topic to configure buckets')
        sys.exit(0)

# Configure buckets
print('Configuring buckets')
for bucket in bucket_list:
    #print(bucket)
    response = s3_client.get_bucket_notification_configuration(
                        Bucket=bucket)
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            "TopicConfigurations": [
               {
                    "Id": "Send notification for changes in bucket area cvmfs",
                    "TopicArn": TOPIC_ARN,
                    "Events": [
                        "s3:ObjectCreated:*","s3:ObjectRemoved:*"
                    ],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {
                                    "Name": "prefix",
                                    "Value": "cvmfs"
                                }
                            ]
                        }
                    }
               },
            ]
        }
)

print('CONFIGURATION COMPLETED')
