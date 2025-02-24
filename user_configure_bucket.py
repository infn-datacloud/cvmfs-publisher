#!/usr/bin/python3

import boto3
import sys
from rgwadmin import RGWAdmin
import argparse 
import botocore
import requests
import logging

# Syslog logging
LOGGER = logging.getLogger('cvmfs_publish')
FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
LOGGER.setLevel(logging.INFO)
logg_operator = 'bucket-configuration.log'
HANDLER = logging.FileHandler(logg_operator,
                                       mode='w', encoding='utf-8', delay=True)
HANDLER.setFormatter(FORMATTER)
HANDLER.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER)
# Get input
PARSER = argparse.ArgumentParser()
PARSER.add_argument('-us', '--user_name', dest='us',
                    help='user name')
ARGS = PARSER.parse_args()
BUCKET = ARGS.us

def device_code(clientid, clientsecret, issurl='https://iam.cloud.infn.it/'):
    payload = {'scope': 'openid profile',
               'client_id': clientid
               }
    response = requests.post(issurl+'/devicecode',
            params=payload,
            auth=(clientid, clientsecret),
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            )
    return response

def device_token(devicecode, clientid, clientsecret, issurl='https://iam.cloud.infn.it/'):
    payload = {'device_code': devicecode,
               'audience': 'object',
               'grant_type': 'urn:ietf:params:oauth:grant-type:device_code'}
    response = requests.post(issurl+'/token',
            params=payload,
            auth=(clientid, clientsecret),
            )
    return response

if __name__ == '__main__':

    # Register client
    if not BUCKET:
        BUCKET = input('Insert you username (mrossi)... ')
    user_data =  {
        "client_name": BUCKET,
        "grant_types": [
            "urn:ietf:params:oauth:grant-type:device_code",
            ],
        }
    url = 'https://iam.cloud.infn.it/iam/api/client-registration'
    headers = {
        "accept" : "application/json" , 
        "content-type" : "application/json",
        }
    r = requests.post(url, json=user_data, headers=headers)
    if r.ok:
        LOGGER.info(f'Status code {r.status_code} : response ok')
        cid = r.json()['client_id']
        csecret = r.json()['client_secret']
        # Get token 
        devcode = device_code(cid, csecret)
        #print(devcode.json())
        input("authorize the device by visiting {} and using the code {} then press enter after the process is complete.".format(devcode.json()['verification_uri'], devcode.json()['user_code']))
        dev2auth = device_token(devcode.json()['device_code'], cid, csecret)
        access_token = dev2auth.json()
        TOKEN = access_token['access_token']

        try:
            sts_client = boto3.client('sts',
                    endpoint_url="https://rgw.cloud.infn.it:443",
                    region_name=''
                    )     

            response = sts_client.assume_role_with_web_identity(
                    RoleArn="arn:aws:iam:::role/IAMaccess",
                    RoleSessionName='Bob',
                    DurationSeconds=3600,
                    WebIdentityToken = TOKEN 
                    )

            s3client = boto3.client('s3',
                    aws_access_key_id = response['Credentials']['AccessKeyId'],
                    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
                    aws_session_token = response['Credentials']['SessionToken'],
                    endpoint_url="https://rgw.cloud.infn.it:443",
                    region_name='default',
                    )

            # Bucket configuration
            arn = f'arn:aws:sns:bbrgwzg::{BUCKET}'
            s3client.put_bucket_notification_configuration(
                    Bucket=BUCKET,
                    NotificationConfiguration={
                        "TopicConfigurations": [
                           {
                                "Id": "Send notification for changes in bucket area cvmfs",
                                "TopicArn": arn,
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
            print(f'Configuration concluded for user bucket {BUCKET}')
            LOGGER.info(f'Successfull bucket configuration for {BUCKET}, now populate the bucket to distribute software in the correspondent CVMFS repo')

        except botocore.exceptions.ClientError as ex:
            LOGGER.warning(ex)
            print(ex)
        except Exception as ex:
            LOGGER.warning(ex)
            print(ex)

    else:
        LOGGER.warning(f'Status code: {r.status_code}')

