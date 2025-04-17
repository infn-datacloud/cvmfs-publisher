# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import logging.handlers
import argparse
import configparser
import os
import boto3
import botocore
import subprocess
import shutil

# Input arguments
PARSER = argparse.ArgumentParser()
PARSER.add_argument('-cfg', '--conf_file', dest='cfg',
                    help='configuration file')
PARSER.add_argument('-wdir', '--working_dir', dest='wdir',
                    help='full path to working directory')
PARSER.add_argument('-b', '--bucket', dest='b',
                    help='bucket to synchronize')
SUBPARSER = PARSER.add_subparsers(help='exclude some diresctories from sync')
PARSER_EXCLUDE = SUBPARSER.add_parser('exclude', help='exclude directories')
PARSER_EXCLUDE.add_argument('--dir', nargs='+', help='directories to not sync')
PARSER_EXCLUDE.add_argument('--file', nargs='+', help='files to not sync')
ARGS = PARSER.parse_args()
# Syslog logging
SYSLOG_LOGGER = logging.getLogger('syslog')
FORMATTER = logging.Formatter(' %(levelname)s - %(message)s')
SYSLOG_HANDLER = logging.handlers.SysLogHandler(address='/dev/log')
SYSLOG_HANDLER.setFormatter(FORMATTER)
SYSLOG_LOGGER.addHandler(SYSLOG_HANDLER)
SYSLOG_LOGGER.setLevel(logging.INFO)
# Operator logging
OPERATOR_LOGGER = logging.getLogger('syncrepo')
OPERATOR_FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
OPERATOR_LOGGER.setLevel(logging.INFO)
logg_operator = f'{ARGS.wdir}/sync-repo.log'
OPERATOR_HANDLER = logging.FileHandler(logg_operator,
                                           mode='w', encoding='utf-8', delay=True)
OPERATOR_HANDLER.setFormatter(OPERATOR_FORMATTER)
OPERATOR_LOGGER.addHandler(OPERATOR_HANDLER)
# Read variables
CONFIG = configparser.ConfigParser()
CONFIG.read(f'{ARGS.wdir}{ARGS.cfg}')
# Rgw parameters
ACCESS_KEY = CONFIG.get('ceph-rgw','access_key')
SECRET_KEY = CONFIG.get('ceph-rgw','secret_key')
ENDPOINT = CONFIG.get('ceph-rgw','url')
ROLE = CONFIG.get('ceph-rgw','role')


def extract_objects():

    try:
        # Connect to Ceph object storage
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

        try:
            tarballs = []
            objects = []
            resp = s3_client.list_objects(
                        Bucket=ARGS.b,
                        Prefix='cvmfs/'
                    )
            if 'Contents' in resp :
                for object in resp['Contents'] :
                    if 'to_extract' in object['Key'] :
                        tarballs.append(object['Key'])
                    else:
                        objects.append(object['Key'])



        except Exception as ex:
            SYSLOG_LOGGER.warning(f'[{ARGS.b}] - {ex}')
            OPERATOR_LOGGER.warning(f'[{ARGS.b}] - {ex}')

    except botocore.exceptions.ClientError as ex:
        SYSLOG_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
        OPERATOR_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
    except Exception as ex:
        SYSLOG_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
        OPERATOR_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')

    return tarballs , objects


def sync_tar(tarballs):

    SYSLOG_LOGGER.info(f'Starting synchronization {ARGS.b} repository - tarball ingestion')
    OPERATOR_LOGGER.info(f'Starting synchronization {ARGS.b} repository - tarball ingestion')
    try:
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

        os.makedirs(f'{ARGS.wdir}software', exist_ok=True)
        for tar in tarballs:
            fullpath = tar.split('/')
            tarb = fullpath[-1]
            user_path = '/'.join(fullpath[2:-1])
            os.makedirs(
                f'{ARGS.wdir}software/{user_path}', exist_ok=True)
            s3_client.download_file(
                        ARGS.b, tar, f'{ARGS.wdir}software/{user_path}/{tarb}')
            cmd = f'cvmfs_server transaction {ARGS.b}.infn.it'
            proc = subprocess.run(cmd, capture_output=True,
                          text=True, shell=True, check=False)
            os.makedirs(f'/cvmfs/{ARGS.b}.infn.it/{user_path}', exist_ok=True)
            cmd = f'cvmfs_server publish {ARGS.b}.infn.it'
            proc = subprocess.run(cmd, capture_output=True,
                          text=True, shell=True, check=False)
            cmd = f'cvmfs_server ingest --tar_file \
                    {ARGS.wdir}software/{user_path}/{tarb} \
                    --base_dir {user_path}/ {ARGS.b}.infn.it'
            proc = subprocess.run(cmd, capture_output=True,
                                  text=True, shell=True, check=False)
            if proc.returncode != 0:
                SYSLOG_LOGGER.warning(f'[{ARGS.b}] - {proc.stderr}')
                OPERATOR_LOGGER.warning(f'[{ARGS.b}] - {proc.stderr}')
        shutil.rmtree(f'{ARGS.wdir}software/')
    except botocore.exceptions.ClientError as ex:
        SYSLOG_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
        OPERATOR_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
    except Exception as ex:
        SYSLOG_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
        OPERATOR_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')


def sync_objs(objects):

    SYSLOG_LOGGER.info(f'Starting synchronization {ARGS.b} repository - downloading objects')
    OPERATOR_LOGGER.info(f'Starting synchronization {ARGS.b} repository - downloading objects..')
    try:
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

        cmd = f'cvmfs_server transaction {ARGS.b}.infn.it'
        proc = subprocess.run(cmd, capture_output=True,
            text=True, shell=True, check=False)
        for obj in objects:
            if len(obj.split('/')) > 2 :
                fullpath = obj.split('/')
                file = fullpath[-1]
                path = '/'.join(fullpath[1:-1])
                os.makedirs(f'/cvmfs/{ARGS.b}.infn.it/{path}', exist_ok=True)
                s3_client.download_file(
                            ARGS.b, f'{obj}', f'/cvmfs/{ARGS.b}.infn.it/{path}/{file}')
            else:
                file = '/'.join((obj.split('/'))[1:])
                s3_client.download_file(
                    ARGS.b, f'{obj}', f'/cvmfs/{ARGS.b}.infn.it/{file}')

        cmd = f'cvmfs_server publish {ARGS.b}.infn.it'
        proc = subprocess.run(cmd, capture_output=True,
                          text=True, shell=True, check=False)
        if proc.returncode != 0:
            publ_err = proc.stderr
            cmd = f'cvmfs_server abort -f {ARGS.b}.infn.it'
            proc = subprocess.run(cmd, capture_output=True,
                          text=True, shell=True, check=False)
            SYSLOG_LOGGER.warning(f'[{ARGS.b}] - {publ_err} - aborted : {proc.stdout}')
            OPERATOR_LOGGER.warning(f'[{ARGS.b}] - {publ_err} - aborted : {proc.stdout}')

    except botocore.exceptions.ClientError as ex:
        SYSLOG_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
        OPERATOR_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
    except Exception as ex:
        SYSLOG_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
        OPERATOR_LOGGER.warning(f'Cannot synchronize CVMFS repository: {ex}')
    return


def main(exclude_dirs,exclude_files):
    cmd = f'cvmfs_server transaction {ARGS.b}.infn.it'
    proc = subprocess.run(cmd, capture_output=True,
            text=True, shell=True, check=False)
    if exclude_dirs or exclude_files:
        for name in os.listdir(f'/cvmfs/{ARGS.b}.infn.it') :
            if os.path.isdir(f'/cvmfs/{ARGS.b}.infn.it/{name}') and name not in exclude_dirs:
                shutil.rmtree(f'/cvmfs/{ARGS.b}.infn.it/{name}', ignore_errors=True)
            elif os.path.isfile(f'/cvmfs/{ARGS.b}.infn.it/{name}') and name not in exclude_files:                   
                os.remove(f'/cvmfs/{ARGS.b}.infn.it/{name}')
    else:
        shutil.rmtree(f'/cvmfs/{ARGS.b}.infn.it', ignore_errors=True)
    cmd = f'cvmfs_server publish {ARGS.b}.infn.it'
    proc = subprocess.run(cmd, capture_output=True,
        text=True, shell=True, check=False)
    if proc.returncode != 0:
        publ_err = proc.stderr
        cmd = f'cvmfs_server abort -f {ARGS.b}.infn.it'
        proc = subprocess.run(cmd, capture_output=True,
            text=True, shell=True, check=False)
        SYSLOG_LOGGER.warning(f'[{ARGS.b}] - {publ_err} - aborted : {proc.stdout}')
        OPERATOR_LOGGER.warning(f'[{ARGS.b}] - {publ_err} - aborted : {proc.stdout}')

    tarballs , objects = extract_objects()
    if tarballs:
        sync_tar(tarballs)

    if objects:
        sync_objs(objects)
    SYSLOG_LOGGER.info(f'Synchronization ended - {ARGS.b}')
    OPERATOR_LOGGER.info(f'Synchronization ended - {ARGS.b}')
    return

if __name__ == '__main__' :
    exclude_dirs = []
    exclude_files = []
    try:
        for path in ARGS.dir :
            exclude_dirs.append(path)
    except:
        print('No directories to exclude')
    try:
        for f in ARGS.file:
            exclude_files.append(f)
    except:
        print('No files to exclude')
    main(exclude_dirs,exclude_files)
