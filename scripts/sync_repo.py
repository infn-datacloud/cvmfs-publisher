#!/usr/bin/python3

import logging
import os
import boto3
import botocore
import subprocess
import shutil
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import json


# Load parameters from JSON file
# This file should contain the necessary parameters for the synchronization such as access keys, bucket name, and working directory.
with open("sync_repo_params.json") as json_data_file:
    data = json.load(json_data_file)

RGW_ACCESS_KEY              = data["ceph-rgw"]['access_key']
RGW_SECRET_KEY              = data["ceph-rgw"]['secret_key']
RGW_ROLE                    = data["ceph-rgw"]['role']
RGW_ENDPOINT                = data["ceph-rgw"]['url']
RGW_REGION                  = data["ceph-rgw"]['region']
wdir                        = data["sync"]['working_dir']
bucket                      = data["sync"]['bucket']
exclude_dirs                = data["sync"]['exclude_dirs']
exclude_files               = data["sync"]['exclude_files']


# Generate log file with current date and weekly rotation
def setup_logging():
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_file = f"/var/log/publisher/sync-repo-{date_stamp}.log"
    logging.basicConfig(
     level=logging.INFO,                    # Logging level: INFO, ERROR, DEBUG
     format='%(asctime)s - %(levelname)s - %(message)s',
     handlers=[TimedRotatingFileHandler(log_file, when='D', interval=7)]
    )

# S3 client
def s3_client():
    sts_client = boto3.client(
        'sts',
        aws_access_key_id=RGW_ACCESS_KEY,
        aws_secret_access_key=RGW_SECRET_KEY,
        endpoint_url= RGW_ENDPOINT,
        region_name = RGW_REGION
        )
    response = sts_client.assume_role(
        RoleArn=f'arn:aws:iam:::role/{RGW_ROLE}',
        RoleSessionName='Bob',
        DurationSeconds=3600
        )
    s3 = boto3.client(
        's3',
        aws_access_key_id = response['Credentials']['AccessKeyId'],
        aws_secret_access_key = response['Credentials']['SecretAccessKey'],
        aws_session_token = response['Credentials']['SessionToken'],
        endpoint_url=RGW_ENDPOINT,
        region_name=RGW_REGION
        )
    return s3


def extract_objects():
    try:    
        # Connect to Ceph object storage
        s3=s3_client()
        tarballs = []
        objects = []
        try:
            resp = s3.list_objects(Bucket=bucket,Prefix='cvmfs/')
            if 'Contents' in resp :
                for object in resp['Contents'] :
                    if object['Key'].endswith('.tar'):
                        tarballs.append(object['Key'])
                    else:
                        objects.append(object['Key'])

        except Exception as ex:
            logging.warning(f'[{bucket}] - {ex}')

    except botocore.exceptions.ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchBucket':
            logging.warning(f'[{bucket}] - Bucket does not exist.')
        elif ex.response['Error']['Code'] == 'AccessDenied':
            logging.warning(f'[{bucket}] - Access denied to the bucket.')
        else:
            logging.warning(f'Cannot synchronize CVMFS repository: {ex}')
    except Exception as ex:
        logging.warning(f'Cannot synchronize CVMFS repository: {ex}')

    return tarballs , objects


def sync_tar(tarballs):
    logging.info(f'Synchronization process for {bucket} S3 bucket - tarball ingestion')
    try:
        # Connect to Ceph object storage
        s3=s3_client()
        os.makedirs(f'{wdir}_software', exist_ok=True)
        for tar in tarballs:
            fullpath = tar.split('/')
            tarb = fullpath[-1]
            user_path = '/'.join(fullpath[2:-1])
            os.makedirs(f'{wdir}_software/{user_path}', exist_ok=True)
            s3.download_file(bucket, tar, f'{wdir}_software/{user_path}/{tarb}')
            logging.info(f'Downloaded {tarb} to {wdir}_software local folder.')
            cmd = f'cvmfs_server transaction {bucket}.infn.it'
            proc = subprocess.run(cmd, capture_output=True,text=True, shell=True, check=False)
            os.makedirs(f'/cvmfs/{bucket}.infn.it/{user_path}', exist_ok=True)
            cmd = f'cvmfs_server publish {bucket}.infn.it'
            proc = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False)
            cmd = f'cvmfs_server ingest --tar_file \
                    {wdir}_software/{user_path}/{tarb} \
                    --base_dir {user_path}/ {bucket}.infn.it'
            logging.info(f'Ingesting {tarb} tarball into {bucket}.infn.it repository ...')
            proc = subprocess.run(cmd, capture_output=True,
                                  text=True, shell=True, check=False)
            logging.info(f'[{bucket}] - {tarb} tarball ingested.')
            if proc.returncode != 0:
                logging.warning(f'[{bucket}] - {proc.stderr}')

        shutil.rmtree(f'{wdir}_software/')

    except botocore.exceptions.ClientError as ex:
        logging.warning(f'Cannot synchronize CVMFS repository: {ex}')
    except Exception as ex:
        logging.warning(f'Cannot synchronize CVMFS repository: {ex}')


def sync_objs(objects):
    try:
        # Connect to Ceph object storage
        s3=s3_client()
        cmd = f'cvmfs_server transaction {bucket}.infn.it'
        logging.info(f'CVMFS transaction for {bucket}.infn.it repository started.')
        proc = subprocess.run(cmd, capture_output=True,
            text=True, shell=True, check=False)
        for obj in objects:
            fullpath = obj.split('/')
            file = fullpath[-1]
            path = '/'.join(fullpath[1:-1])
            if len(obj.split('/')) > 2 :
                os.makedirs(f'/cvmfs/{bucket}.infn.it/{path}', exist_ok=True)
                s3.download_file(bucket, f'{obj}', f'/cvmfs/{bucket}.infn.it/{path}/{file}')
            else:
                file = '/'.join((obj.split('/'))[1:])
                s3.download_file(bucket, f'{obj}', f'/cvmfs/{bucket}.infn.it/{file}')
            if path=="":
                logging.info(f'Downloaded {obj} to /cvmfs/{bucket}.infn.it/{file}.')
            else:
                logging.info(f'Downloaded {obj} to /cvmfs/{bucket}.infn.it/{path}/{file}.')
        cmd = f'cvmfs_server publish {bucket}.infn.it'
        logging.info(f'CVMFS publish for {bucket}.infn.it repository started')
        proc = subprocess.run(cmd, capture_output=True,
                          text=True, shell=True, check=False)
        if proc.returncode != 0:
            publ_err = proc.stderr
            cmd = f'cvmfs_server abort -f {bucket}.infn.it'
            logging.warning(f'[{bucket}] - {publ_err} - aborting transaction...')
            proc = subprocess.run(cmd, capture_output=True,
                          text=True, shell=True, check=False)
            logging.warning(f'[{bucket}] - {publ_err} - aborted : {proc.stdout}')

    except botocore.exceptions.ClientError as ex: 
        logging.warning(f'Cannot synchronize CVMFS repository: {ex}')
    except Exception as ex:   
        logging.warning(f'Cannot synchronize CVMFS repository: {ex}')
 


def main(exclude_dirs,exclude_files):
    cmd = f'cvmfs_server transaction {bucket}.infn.it'
    proc = subprocess.run(cmd, capture_output=True,
            text=True, shell=True, check=False)
    if exclude_dirs or exclude_files:
        for name in os.listdir(f'/cvmfs/{bucket}.infn.it') :
            if os.path.isdir(f'/cvmfs/{bucket}.infn.it/{name}') and name not in exclude_dirs:
                shutil.rmtree(f'/cvmfs/{bucket}.infn.it/{name}', ignore_errors=True)
            elif os.path.isfile(f'/cvmfs/{bucket}.infn.it/{name}') and name not in exclude_files:                   
                os.remove(f'/cvmfs/{bucket}.infn.it/{name}')
    else:
        shutil.rmtree(f'/cvmfs/{bucket}.infn.it', ignore_errors=True)
    cmd = f'cvmfs_server publish {bucket}.infn.it'
    proc = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False)
    if proc.returncode != 0:
        publ_err = proc.stderr
        cmd = f'cvmfs_server abort -f {bucket}.infn.it'
        proc = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False)
        logging.warning(f'[{bucket}] - {publ_err} - aborted : {proc.stdout}')

    tarballs , objects = extract_objects()
    if tarballs:
        sync_tar(tarballs)

    if objects:
        sync_objs(objects)

    logging.info(f'Synchronization process for {bucket}/cvmfs bucket and {bucket}.infn.it CVMFS repository ended.')



if __name__ == '__main__' :
    # Setup logging
    setup_logging()
    logging.info(f'Starting synchronization process between {bucket}/cvmfs bucket and {bucket}.infn.it CVMFS repository ...')
    main(exclude_dirs,exclude_files)

