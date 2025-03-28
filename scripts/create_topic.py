#!/usr/bin/python3

import boto3
import argparse 

PARSER = argparse.ArgumentParser()


PARSER.add_argument('-a', '--access-key', dest='a',
                    help='cvmfs-publisher access key')
PARSER.add_argument('-s', '--secret-key', dest='s',
                    help='cvmfs-publisher secret key')
PARSER.add_argument('-u', '--endpoint', dest='u',
                    help='endpoint rgw')
PARSER.add_argument('-us', '--rabbit-user', dest='us',
                    help='rabbit impersonator username')
PARSER.add_argument('-ps', '--password', dest='ps',
                    help='rabbit impersonator password')
PARSER.add_argument('-ip', '--ip-amqp', dest='ip',
                    help='rabbit ip')
PARSER.add_argument('-p', '--port', dest='p',
                    help='rabbit port')
PARSER.add_argument('-v', '--verbose', action='store_true', dest='v',
                    help='increase verbosity')

ARGS = PARSER.parse_args()
ACCESS_KEY = ARGS.a
SECRET_KEY = ARGS.s
ENDPOINT = ARGS.u
AMQP_USER = ARGS.us
AMQP_PSSW = ARGS.ps
AMQP_IP = ARGS.ip
AMQP_PORT = ARGS.p
TOPIC_NAME = 'bucketupdate' # topic name = routing_key parameter = queue name
AMQP_EXCHANGE = 'notification'

if ARGS.v:
    boto3.set_stream_logger(name='botocore')

sns_client = boto3.client('sns',
    aws_access_key_id = ACCESS_KEY,
    aws_secret_access_key = SECRET_KEY,
    endpoint_url= ENDPOINT,
    region_name='default',
    )

#List topics
#input('Listing topics')
#resp = sns_client.list_topics()
#print(resp)


# Delete topics
#input('Deleting topics')
#resp = sns_client.delete_topic(
#    TopicArn=''
#)
#input()

## Create a topic
input('Creating topic')
attributes = {'push-endpoint' : f'amqps://{AMQP_USER}:{AMQP_PSSW}@{AMQP_IP}:{AMQP_PORT}' , 'amqp-exchange': AMQP_EXCHANGE, 'amqp-ack-level': 'broker', 'verify-ssl':'false' , 'use-ssl' : 'true' , 'persistent' : 'true'}
resp = sns_client.create_topic(Name= TOPIC_NAME,
                                Attributes=attributes)
topic_arn = resp["TopicArn"]
#topic_arn=''
#topic_arn='arn:aws:sns:ceph-objectstore::bucketupdate'
print(topic_arn)
input('Topic created')
