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
CA_CERT                     = data["ssl"]['ca_cert']
CLIENT_CERT                 = data["ssl"]['client_cert']
CLIENT_KEY                  = data["ssl"]['client_key']
V_URL                       = data["vault"]['vault_url']
V_ROLEID                    = data["vault"]['role_id']
V_SECRETID                  = data["vault"]['secret_id']



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
        print(f'Topic created for repo {repo}, topic_arn = {topic_arn}')
        logging.info(f'Topic created for repo {repo}, topic_arn = {topic_arn}')

    except Exception as ex:
        print(f'An unexpected error occurred: {ex}')
        logging.error(f'An unexpected error occurred: {ex}')

    return True



def create_queue(channel, repo):
    
    repo = repo.split('.')[0]
    try:
        channel.queue_declare(queue=repo, durable=True, arguments={"x-queue-type": "quorum"}, exclusive=False)
        channel.queue_bind(exchange = RMQ_EXCHANGE, queue = repo, routing_key= repo)

        print(f'Queue {repo} created for repo {repo}.infn.it')
        logging.info(f'Queue {repo} created for repo {repo}.infn.it')
        return True

    except pika.exceptions.ConnectionClosed as ex:
        print(f'RabbitMQ client unreachable or dead: {ex}')
        logging.warning(f'RabbitMQ client unreachable or dead: {ex}')
        return False
    except pika.exceptions.StreamLostError as ex:
        print(f'RabbitMQ lost connection: {ex}')
        logging.warning(f'RabbitMQ lost connection: {ex}')
        return False
    except Exception as ex:
        print(f'An unexpected error occurred: {ex}')
        logging.warning(f'An unexpected error occurred: {ex}')
        return False



# VAUL AppRole login method
def vault_login_approle(client):
    try:
        login_response = client.auth.approle.login(role_id=V_ROLEID,secret_id=V_SECRETID)
        print("Login to Vault server successful.")
        logging.info("Login to Vault server successful.")

    except hvac.exceptions.InvalidRequest as e:
        print("Invalid request error:", e)
        logging.info("Invalid request error:", e)

    except hvac.exceptions.Forbidden as e:
        print("Access forbidden:", e)
        logging.info("Access forbidden:", e)

    except hvac.exceptions.VaultError as e:
        # Generic Vault-related error
        print("Vault error occurred:", e)
        logging.info("Vault error occurred:", e)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logging.info(f"An unexpected error occurred: {e}")


'''Retrive CVMFS repo keys from vault '''
def get_repo_keys(msg):
    # delcorso,34158350-c746-4918-82ab-9004dd03f95b,repo32.infn.it,G
    subject = msg.split(',')[1]
    repository_name=msg.split(',')[2]
    type_repo = msg.split(',')[3]

    try:
        client = hvac.Client(V_URL)
        vault_login_approle(client)
        
        print("VAULT client authenticated:",client.is_authenticated())
        logging.info(f"VAULT client authenticated: {client.is_authenticated()}.")
        print('Vault initialize status: %s' % client.sys.is_initialized())
        logging.info(f"Vault initialize status: {client.sys.is_initialized()}. ")
        print("Vault is sealed:", client.sys.is_sealed())
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

    except Exception as ex:
        print(f'{ex}')
        logging.warning(f'{ex}')
        return False
    
    return repository_name


'''Make a CVMFS repository writable from publisher via gateway'''
def create_repo_publisher(repo_name):
    
    cmd = f'sudo cvmfs_server mkfs -w {CVMFS_SERVER_URL}{repo_name} \
    -u gw,/srv/cvmfs/{repo_name}/data/txn,{CVMFS_UP_STORAGE} \
    -k /tmp/{repo_name}_keys -o `whoami` {repo_name}'
    try:

       subprocess.run(cmd, shell=True, capture_output=True, check=True)
       
       print(f'CVMFS repository {repo_name} successfully created.')
       logging.info(f'CVMFS repository {repo_name} successfully created.')
       shutil.rmtree(f'/tmp/{repo_name}_keys/')
       return True
   
    except subprocess.CalledProcessError as e:
        print(f"{e.stderr.decode()}")
        logging.info(f"{e.stderr.decode()}")
        stderr_output = e.stderr.decode()
        # Case repo already exists 
        if "already exists" in stderr_output:
            print("CVMFS repo not created.")
            logging.info("CVMFS repo not created.")
            shutil.rmtree(f'/tmp/{repo_name}_keys/')
            return True
        else:
            return False

    except Exception as e:
        print(f"Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}")
        return False

'''Function called whenever a message from publisher queue is received'''
# sent from the INFN dashboard or via CLI from the cvmfs_repo_agent.py script on the stratum0 when a user requires a new cvmfs personal/group repository.
def callback(ch, method, properties, body):
    
    message=body.decode("utf-8")
    print(f' [*] {message} ')               #delcorso,34158350-c746-4918-82ab-9004dd03f95b,repo32.infn.it,G
    repo_name = get_repo_keys(message)
    if repo_name is not False:
        res = create_repo_publisher(repo_name)
        if res is not True:
            print(f'Cannot create CVMFS repo in publisher: {res}. Message NOT acknowledged.')
            logging.warning(f'Cannot create CVMFS repo in publisher: {res}. Message NOT acknowledged.')
        else:
            create_t=create_topic(repo_name)
            if create_t is not True:
                print(f'Cannot create topic for the CVMFS repo in publisher: {create_t}. Message NOT acknowledged.')
                logging.warning(f'Cannot create topic for the CVMFS repo in publisher: {create_t}. Message NOT acknowledged.')
            else:
                create_q=create_queue(ch, repo_name)
                if create_q is not True:
                    print(f'Cannot create queue for the CVMFS repo in publisher: {create_q}. Message NOT acknowledged. ')
                    logging.warning(f'Cannot create queue for the CVMFS repo in publisher: {create_q}. Message NOT acknowledged.')
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

def log_generation():
    # Generate log file with current date
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"/var/log/publisher/publisher_consumer-{date_stamp}.log"
    logging.basicConfig(
         level=logging.INFO,                    # Set the logging level: INFO, ERROR, DEBUG
         filename=log_filename,                 # Specify log file name
         filemode='a',                          # Append to the file if it exists
         format='%(asctime)s - %(levelname)s - %(message)s'
    )


def main():
        
        log_generation()
        try: 
            context = ssl.create_default_context(cafile=CA_CERT)
            context.load_cert_chain(certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
            
            # Establish connection with RabbitMQ server
            credentials = pika.PlainCredentials(RMQ_RGW_USER,RMQ_RGW_PASSWORD)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RMQ_HOST,
                                                                           port=RMQ_PORT,
                                                                           credentials=credentials,
                                                                           ssl_options=pika.SSLOptions(
                                                                               context, server_hostname=RMQ_HOSTNAME)
                                                                           )
                                                                           )
            print('Connected to RabbitMQ, starting consuming publisher queue...')
            logging.info('Connected to RabbitMQ, starting consuming publisher queue...')
            channel = connection.channel()
            channel.queue_declare(queue=RMQ_PUBLISHER_QUEUE, durable=True, arguments={"x-queue-type": "quorum"})
            channel.basic_qos(prefetch_count=1)
            
            # Tell RabbitMQ that callback function should receive messages from the publisher queue
            channel.basic_consume(queue=RMQ_PUBLISHER_QUEUE,
                                  auto_ack=False,
                                  on_message_callback=callback)
            

            # Enter a never-ending loop that waits for data and runs callbacks whenever necessary
            print(' [*] Waiting for messages. To exit press CTRL+C')
            logging.info(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()

        except pika.exceptions.ConnectionClosed as ex:
            logging.warning(f'RabbitMQ client unreachable or dead: {ex}')
            print(f'RabbitMQ client unreachable or dead: {ex}')
        except pika.exceptions.StreamLostError as ex:
            logging.warning(f'RabbitMQ lost connection: {ex}')
            print(f'RabbitMQ lost connection: {ex}')
        except Exception as ex:
            logging.warning(f'Strange error: {ex}')
            print(f'Strange error: {ex}')



if __name__ == '__main__' :
   
      try:
        main()

      except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0) 

    
