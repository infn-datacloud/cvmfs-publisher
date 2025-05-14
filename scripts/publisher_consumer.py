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

import boto3
import pika
import pika.exceptions
import hvac
import ssl
import json
import argparse
import subprocess
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import shutil
from datetime import datetime
import os,sys
import socket


with open("parameters.json") as json_data_file:
    data = json.load(json_data_file)

CVMFS_SERVER_URL            = data["cvmfs"]["stratum0_url"]
CVMFS_UP_STORAGE            = data["cvmfs"]["upstream_storage"]
RMQ_HOST                    = data["rabbitmq"]['host']
RMQ_PORT                    = data["rabbitmq"]['port']
RMQ_HOSTNAME                = data["rabbitmq"]['hostname']
RMQ_RGW_USER                = data["rabbitmq"]['rgw_user']
RMQ_RGW_PASSWORD            = data["rabbitmq"]['rgw_password']
RMQ_URL                     = data["rabbitmq"]['url']
RMQ_EXCHANGE                = data["rabbitmq"]['exchange']
RMQ_PUBLISHER_QUEUE         = data["rabbitmq"]['publisher_queue']
RGW_ACCESS_KEY              = data["ceph-rgw"]['access_key']
RGW_SECRET_KEY              = data["ceph-rgw"]['secret_key']
RGW_ENDPOINT                = data["ceph-rgw"]['url']
RGW_REGION                  = data["ceph-rgw"]['region']
SSL_CA_CERT                 = data["ssl"]['ca_cert']
SSL_CLIENT_CERT             = data["ssl"]['client_cert']
SSL_CLIENT_KEY              = data["ssl"]['client_key']
V_URL                       = data["vault"]['vault_url']
V_ROLEID                    = data["vault"]['role_id']
V_SECRETID                  = data["vault"]['secret_id']
ZBX_SERVER                  = data["zabbix"]['server']
ZBX_ITEM_KEY                = data["zabbix"]['item_key3']


# Alerts sent to Zabbix server
def send_to_zabbix(message):
    HOSTNAME =socket.gethostname()
    cmd = f'zabbix_sender -z {ZBX_SERVER} -s {HOSTNAME} -k {ZBX_ITEM_KEY} -o "{message}"'
    try:
        subprocess.run(cmd, shell=True)
    except Exception as e:
        logging.error(f"Zabbix notification failed: {e}")


# Generate log file with current date and weekly rotation
def setup_logging():
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_file = f"/var/log/publisher/publisher_consumer-{date_stamp}.log"
    logging.basicConfig(
            level=logging.INFO,                    
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[TimedRotatingFileHandler(log_file, when='D', interval=7)]
    )



def create_topic(repo):
    repo = repo.split('.')[0]
    try:
        sns_client = boto3.client('sns',
        aws_access_key_id = RGW_ACCESS_KEY,
        aws_secret_access_key = RGW_SECRET_KEY,
        endpoint_url= RGW_ENDPOINT,
        region_name=RGW_REGION,
        )
        logging.info('CREATING TOPIC...')
        
        attributes = {'push-endpoint' : f'amqps://{RMQ_RGW_USER}:{RMQ_RGW_PASSWORD}@{RMQ_HOST}:{RMQ_PORT}' , 'amqp-exchange': RMQ_EXCHANGE, 'amqp-ack-level': 'broker', 'verify-ssl':'false' , 'use-ssl' : 'true' , 'persistent' : 'true'}
        resp = sns_client.create_topic(Name= repo, Attributes=attributes)
        topic_arn = resp["TopicArn"]
        logging.info(f'Topic created for repo {repo}, topic_arn = {topic_arn}')
    except Exception as ex:
        error_msg=f'An unexpected error occurred: {ex}'
        logging.error(error_msg)
        send_to_zabbix(error_msg)
    return True


def delete_topic(s,repo):
    repo = repo.split('.')[0]
    try:
        sns_client = boto3.client('sns',
        aws_access_key_id = RGW_ACCESS_KEY,
        aws_secret_access_key = RGW_SECRET_KEY,
        endpoint_url= RGW_ENDPOINT,
        region_name=RGW_REGION,
        )
        arn = f'arn:aws:sns:bbrgwzg::{repo}'
        resp = sns_client.delete_topic(TopicArn=arn)    
        logging.info(f'Topic deleted for repo {repo}')
    except Exception as ex:
        error_msg=f'An unexpected error occurred: {ex}'
        logging.error(error_msg)
        send_to_zabbix(error_msg)
    return True


def create_queue(channel, repo):
    repo = repo.split('.')[0]
    try:
        channel.queue_declare(queue=repo, durable=True, arguments={"x-queue-type": "quorum"}, exclusive=False)
        channel.queue_bind(exchange = RMQ_EXCHANGE, queue = repo, routing_key= repo)
        logging.info(f'Queue {repo} created for repo {repo}.infn.it')
        return True
    except pika.exceptions.ConnectionClosed as ex:
        error_msg=f'RabbitMQ client unreachable or dead: {ex}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False
    except pika.exceptions.StreamLostError as ex:
        error_msg=f'RabbitMQ lost connection: {ex}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False
    except Exception as ex:
        error_msg=f'An unexpected error occurred: {ex}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False



# VAUL AppRole login method
def vault_login_approle(client):
    try:
        login_response = client.auth.approle.login(role_id=V_ROLEID,secret_id=V_SECRETID)
        logging.info("Login to Vault server successful.")
    except hvac.exceptions.InvalidRequest as e:
        error_msg=f'Invalid request error: {e}'
        logging.info(error_msg)
        send_to_zabbix(error_msg)
    except hvac.exceptions.Forbidden as e:
        error_msg=f'Access forbidden: {e}'
        logging.info(error_msg)
        send_to_zabbix(error_msg)
    except hvac.exceptions.VaultError as e:
        error_msg=f'Vault error occurred: {e}'
        logging.info(error_msg)
        send_to_zabbix(error_msg)
    except Exception as e:
        error_msg=f'An unexpected error occurred: {e}'
        logging.info(error_msg)
        send_to_zabbix(error_msg)


# Retrive CVMFS repo keys from vault
def get_repo_keys(msg):
    subject = msg.split(',')[1]
    repository_name=msg.split(',')[2]
    type_repo = msg.split(',')[3]
    try:
        client = hvac.Client(V_URL)
        vault_login_approle(client)
        logging.info(f"VAULT client authenticated: {client.is_authenticated()}.")
        logging.info(f"Vault initialize status: {client.sys.is_initialized()}. ")
        logging.info(f"Vault is sealed: {client.sys.is_sealed()}.")
        if type_repo == 'P':    
            PATH = "secrets/data/"+subject+"/cvmfs_keys/"+repository_name+"/"
        else:
            PATH = "secrets/data/groups/"+repository_name.split('.')[0]+"/cvmfs_keys/"+repository_name+"/"
        read_response = client.read(path=PATH)
        p = Path('/tmp/') / f'{repository_name}_keys'
        p.mkdir(exist_ok=True)
        with (p / f'{repository_name}.crt').open('w') as f:
            f.write(read_response['data']['data']['certificateKey'])
        with (p / f'{repository_name}.gw').open('w') as f:
            f.write(read_response['data']['data']['gatewayKey'])
        with (p / f'{repository_name}.pub').open('w') as f:
            f.write(read_response['data']['data']['publicKey'])
    except Exception as e:
        error_msg=f'{e}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
    return repository_name


# Make a CVMFS repository writable from publisher via gateway
def create_repo_publisher(repo_name):
    
    cmd = f'sudo cvmfs_server mkfs -w {CVMFS_SERVER_URL}{repo_name} \
    -u gw,/srv/cvmfs/{repo_name}/data/txn,{CVMFS_UP_STORAGE} \
    -k /tmp/{repo_name}_keys -o `whoami` {repo_name}'
    try:
       subprocess.run(cmd, shell=True, capture_output=True, check=True)
       logging.info(f'CVMFS repository {repo_name} successfully created.')
       shutil.rmtree(f'/tmp/{repo_name}_keys/')
       return True
    except subprocess.CalledProcessError as e:
        error_msg=f'{e.stderr.decode()}'
        logging.info(error_msg)
        stderr_output = e.stderr.decode()
        # Case repo already exists 
        if "already exists" in stderr_output:
            logging.info("CVMFS repo not created.")
            shutil.rmtree(f'/tmp/{repo_name}_keys/')
            return True
        else:
            send_to_zabbix(error_msg)
            return False
    except Exception as e:
        error_msg=f"Unexpected error: {e}"
        logging.error(error_msg)
        send_to_zabbix(error_msg)
        return False


# Function called whenever a message from publisher queue is received
def callback(ch, method, properties, body):    
    message=body.decode("utf-8")              
    repo_name = get_repo_keys(message)
    if repo_name is not False:
        res = create_repo_publisher(repo_name)
        if res is not True:
            logging.warning(f'Cannot create CVMFS repo in publisher: {res}. Message NOT acknowledged.')
        else:
            create_t=create_topic(repo_name)
            if create_t is not True:
                logging.warning(f'Cannot create topic for the CVMFS repo in publisher: {create_t}. Message NOT acknowledged.')
            else:
                create_q=create_queue(ch, repo_name)
                if create_q is not True:
                    logging.warning(f'Cannot create queue for the CVMFS repo in publisher: {create_q}. Message NOT acknowledged.')
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
        setup_logging()
        try: 
            context = ssl.create_default_context(cafile=SSL_CA_CERT)
            context.load_cert_chain(certfile=SSL_CLIENT_CERT, keyfile=SSL_CLIENT_KEY)
            
            # Establish connection with RabbitMQ server
            credentials = pika.PlainCredentials(RMQ_RGW_USER,RMQ_RGW_PASSWORD)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RMQ_HOST,
                                                                           port=RMQ_PORT,
                                                                           credentials=credentials,
                                                                           ssl_options=pika.SSLOptions(
                                                                               context, server_hostname=RMQ_HOSTNAME)))
            logging.info('Connected to RabbitMQ, starting consuming publisher queue...')
            channel = connection.channel()
            channel.queue_declare(queue=RMQ_PUBLISHER_QUEUE, durable=True, arguments={"x-queue-type": "quorum"})
            channel.basic_qos(prefetch_count=1)
            
            # Tell RabbitMQ that callback function should receive messages from the publisher queue
            channel.basic_consume(queue=RMQ_PUBLISHER_QUEUE,
                                  auto_ack=False,
                                  on_message_callback=callback)
            

            # Enter a never-ending loop that waits for data and runs callbacks whenever necessary
            logging.info(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()

        except pika.exceptions.ConnectionClosed as e:
            error_msg=f'RabbitMQ client unreachable or dead: {e}'
            logging.warning(error_msg)
            send_to_zabbix(error_msg)
        except pika.exceptions.StreamLostError as e:
            error_msg=f'RabbitMQ lost connection: {e}'
            logging.warning(error_msg)
            send_to_zabbix(error_msg)
        except Exception as e:
            error_msg=f"Error in {RMQ_PUBLISHER_QUEUE} queue: {e}"
            logging.warning(error_msg)
            send_to_zabbix(error_msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        error_msg="Shutdown publisher-consumer.py script via KeyboardInterrupt."
        logging.info(error_msg)
        send_to_zabbix(error_msg)
        sys.exit(0)
    except Exception as e:
        error_msg=f"Fatal error in main loop: {e}"
        logging.error(error_msg)
        send_to_zabbix(error_msg)
        sys.exit(1)
