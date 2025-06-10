# Author: FDC
# @ 30 May 2025

import os
import sys
import subprocess
import socket
import pika
import threading
import time
import ssl
import json
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError, BotoCoreError
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import requests
import hvac
#from pathlib import Path


with open("parameters.json") as json_data_file:
    data = json.load(json_data_file)

RMQ_HOST                    = data["rabbitmq"]['host']
RMQ_PORT                    = data["rabbitmq"]['port']
RMQ_USER                    = data["rabbitmq"]['admin_user']
RMQ_PASSWORD                = data["rabbitmq"]['admin_password']
RMQ_URL                     = data["rabbitmq"]['url']
RMQ_EXCLUDED_QUEUES         = data["rabbitmq"]['excluded_queues']
RGW_ACCESS_KEY              = data["ceph-rgw"]['access_key']
RGW_SECRET_KEY              = data["ceph-rgw"]['secret_key']
RGW_ROLE                    = data["ceph-rgw"]['role']
RGW_ENDPOINT                = data["ceph-rgw"]['url']
RGW_REGION                  = data["ceph-rgw"]['region']
SSL_CA_CERT                 = data["ssl"]['ca_cert']
SSL_CLIENT_CERT             = data["ssl"]['client_cert']
SSL_CLIENT_KEY              = data["ssl"]['client_key']
ZBX_SERVER                  = data["zabbix"]['proxy_server']
ZBX_ITEM_KEY                = data["zabbix"]['item_key1']
V_URL                       = data["vault"]['vault_url']
V_ROLEID                    = data["vault"]['role_id']
V_SECRETID                  = data["vault"]['secret_id']
PREFETCH_COUNT              = 10
CHECK_INTERVAL              = 1800 # 30 minutes
RUNNING_THREADS             = {}


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
    log_file = f"/var/log/publisher/cvmfs_repo_consumers-{date_stamp}.log"
    logging.basicConfig(
     level=logging.INFO,                    # Logging level: INFO, ERROR, DEBUG
     format='%(asctime)s - %(levelname)s - %(message)s',
     handlers=[TimedRotatingFileHandler(log_file, when='D', interval=7)]
    )



# SSL context for secure connection
def create_ssl_context():
    context = ssl.create_default_context(cafile=SSL_CA_CERT)
    context.load_cert_chain(certfile=SSL_CLIENT_CERT, keyfile=SSL_CLIENT_KEY)
    context.check_hostname = False        
    context.verify_mode = ssl.CERT_NONE 
    return context



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


# VAUL AppRole login method
def vault_login_approle(client):
    try:
        client.auth.approle.login(role_id=V_ROLEID,secret_id=V_SECRETID)
        logging.info("Login to Vault server successful.")
    except hvac.exceptions.InvalidRequest as e:
        error_msg=f'Invalid request error: {e}'
        logging.error(error_msg)
        send_to_zabbix(error_msg)
    except hvac.exceptions.Forbidden as e:
        error_msg=f'Access forbidden: {e}'
        logging.error(error_msg)
        send_to_zabbix(error_msg)
    except hvac.exceptions.VaultError as e:
        error_msg=f'Vault error occurred: {e}'
        logging.error(error_msg)
        send_to_zabbix(error_msg)
    except Exception as e:
        error_msg=f'An unexpected error occurred: {e}'
        logging.error(error_msg)
        send_to_zabbix(error_msg)


# Retrive CVMFS repo keys from vault and copy them in /data/cvmfs/{repository_name}/keys
def get_repo_keys(bucket, principalId):
    try:
        client = hvac.Client(V_URL)
        vault_login_approle(client)
        repository_name=bucket+".infn.it"
        # we don't know if the repo is personal or group 
        PATH = "secrets/data/"+principalId+"/cvmfs_keys/"+repository_name+"/"
        read_response = client.read(path=PATH)
        if read_response is None:
            # case group repo (otherwise case personal repo)
            PATH = "secrets/data/groups/"+repository_name.split('.')[0]+"/cvmfs_keys/"+repository_name+"/"
            read_response = client.read(path=PATH)
        # Save keys as files
        fileExt=['pub','gw','crt']
        key_list = ['publicKey','gatewayKey','certificateKey']
        for i in range(len(key_list)):
            keys=f"/data/cvmfs/{repository_name}/keys/{bucket}.infn.it.{fileExt[i]}"
            with open(keys, 'w') as file:
                file.write(read_response['data']['data'][key_list[i]])
                file.close()
    except Exception as e:
        error_msg=f'{e}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
    return repository_name


def process_messages(message):
    try:
        msg=json.loads(message.decode("utf-8"))
        bucket = msg['Records'][0]['s3']['bucket']['name']                              # bucket=repo01
        key = msg['Records'][0]['s3']['object']['key']                                  # key=cvmfs/netCDF-92
        Operation = msg['Records'][0]['eventName']                                      # Operation=ObjectCreated:Put ==> download
        principalId = msg['Records'][0]['s3']['bucket']['ownerIdentity']['principalId'] # principalId=34158350-c746-4918-82ab-9004dd03f95b
        logging.info(f"Operation: {Operation}, Bucket: {bucket}, Key: {key}, principalId: {principalId}")
        dir_file, filename = os.path.split(key)                                         # dir_file=cvmfs, filename=netCDF-92        
        full_path=f"/data/cvmfs/{bucket}.infn.it{dir_file[5:]}/{filename}"
        base_path = os.path.dirname(full_path)                                          # base_path=/data/cvmfs/repo17.infn.it
        delete_dir = os.path.join(base_path, 'to_delete')
        extract_dir= os.path.join(base_path, 'to_extract')
        keys_dir= os.path.join(base_path, 'keys')
        # Create the directory for the file path (ignoring the file itself if present), even with delete operation
        if not os.path.exists(base_path):
                os.makedirs(base_path)
                logging.info(f"Directory {base_path} created successfully.")
        if not os.path.exists(delete_dir):
                os.makedirs(delete_dir)
                logging.info(f"Directory {delete_dir} created successfully.")
        if not os.path.exists(extract_dir):
                os.makedirs(extract_dir)
                logging.info(f"Directory {extract_dir} created successfully.")
        if not os.path.exists(keys_dir):
                os.makedirs(keys_dir)
                logging.info(f"Directory {keys_dir} created successfully.")
        
        # Get repo keys from Vault for the CVMFS repo
        get_repo_keys(bucket, principalId)

        # UPLOAD files and .tar files
        if ("ObjectCreated" in Operation):
             if filename.endswith('.tar'):
                 to_extract_filename="/data/cvmfs/" + bucket + ".infn.it" + "/to_extract" + dir_file[5:] + "/" + filename
                 return download_from_s3(bucket, key, to_extract_filename)
             else: 
                 return download_from_s3(bucket, key, full_path)
        else:
             # DELETE operation
             if ("ObjectRemoved" in Operation):
                # The file to be removed (file_to_be_removed) from CVMFS repo is written in a .txt file (to_delete_file) located under the to_delete folder
                file_to_be_removed= "/cvmfs/" + bucket + ".infn.it" + dir_file[5:] + "/" + filename
                to_delete_file = base_path + "/to_delete/" + bucket + "-infn-it.txt"
                with open(to_delete_file, "a") as f:
                    f.write(file_to_be_removed + "\n")
                return True
             else:
                logging.info("Operation not supported.")

    except Exception as e:
            error_msg=f"Failed to process message: {str(e)}"
            logging.warning(error_msg)    
            send_to_zabbix(error_msg)
    return False



def download_from_s3(bucket, key, Filename):    
    s3=s3_client()
    try:        
        s3.download_file(bucket, key, Filename)
        logging.info(f"Successfully downloaded {key} to {Filename}.")
        return True

    except FileNotFoundError:
        logging.info(f'FileNotFound ERROR. Filename={Filename}, bucket={bucket}, key={key}.')
        # Check if the file exists and if not exists, download it in /tmp
        if not os.path.exists(os.path.dirname(Filename)):
           logging.info("Download path not found. Defaulting to /tmp.")
           download_path = os.path.join('/tmp', os.path.basename(Filename))
           try:
               s3.download_file(bucket, key, download_path)
               logging.info(f"File {key} downloaded successfully to {download_path}")
               return True
           except ClientError as e:
               error_msg=f'File {key} NOT downloaded successfully to {download_path}. Error: {e}'
               logging.info(error_msg)
               send_to_zabbix(error_msg)
               return False
        else:
            return False

    except NoCredentialsError:
        error_msg='Credentials not available.'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False
    except PartialCredentialsError:
        error_msg='Incomplete credentials provided.'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # No download for an S3 non existing file. This case is not considered an error
            logging.warning(f'The object {key} does not exist in the bucket {bucket}. Not downloaded.')
            return True
        else:
            error_msg=f'Client error: {e}'
            logging.warning(error_msg)   
            send_to_zabbix(error_msg)
            return False
    except BotoCoreError as e:
        error_msg=f'BotoCoreError occurred: {e}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False
    except Exception as e:
        error_msg=f'An unexpected error occurred: {e}'
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        return False



# RabbitMQ connection setup
def connect_rabbitmq():
    credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASSWORD)
    ssl_context = create_ssl_context()
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=RMQ_HOST,
        port=RMQ_PORT,
        credentials=credentials,
        ssl_options=pika.SSLOptions(context=ssl_context), 
        heartbeat=3600, 
        blocked_connection_timeout=300,
        retry_delay=5, 
        connection_attempts=3
        ))



# Consumer callback
def callback(ch, method, properties, body):
    if process_messages(body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"Acked: {method.routing_key}")
    else:
        error_msg=f"Failed processing. Not acked: {method.routing_key}."
        logging.warning(error_msg)
        send_to_zabbix(error_msg)



# Worker thread per queue
def worker(queue_name):
    try:
        conn = connect_rabbitmq()
        ch = conn.channel()
        ch.basic_qos(prefetch_count=PREFETCH_COUNT)
        ch.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type':'quorum'})
        ch.basic_consume(queue=queue_name, on_message_callback=callback)
        logging.info(f"[✓] Listening on: {queue_name}")
        ch.start_consuming()
    except Exception as e:
        error_msg=f"Error in {queue_name} queue thread: {e}"
        logging.error(error_msg)
        send_to_zabbix(error_msg)



# Getting RabbitMQ queues
def get_queues():
    try:
        url = f'{RMQ_URL}/api/queues'
        requests.packages.urllib3.disable_warnings()
        resp = requests.get(url, auth=(RMQ_USER, RMQ_PASSWORD), verify=False)
        
        if resp.status_code == 200:
            return [q['name'] for q in resp.json() if q['name'] not in RMQ_EXCLUDED_QUEUES and 'amq.gen' not in  q['name']]
        else:
            logging.info(f"Failed to fetch queues. Status code: {resp.status_code}")
            return []

    except Exception as e:
       error_msg=f"Error while connecting to RabbitMQ API: {e}"
       logging.error(error_msg)
       send_to_zabbix(error_msg)
       return []


# Monitor worker threads
def monitor_threads():
    while True:
        logging.info("Verify active queues ...")
        for queue in get_queues():
            if queue not in RUNNING_THREADS or not RUNNING_THREADS[queue].is_alive():
                logging.info(f"Starting thread for queue: {queue}")
                thread = threading.Thread(target=worker, args=(queue,), daemon=True)
                RUNNING_THREADS[queue] = thread
                thread.start()
        time.sleep(CHECK_INTERVAL)



def main():    
    setup_logging()        
    monitor_threads()
    


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        error_msg="Shutdown cvmfs_repo_consumers.py script via KeyboardInterrupt."
        logging.warning(error_msg)
        send_to_zabbix(error_msg)
        sys.exit(0)
    except Exception as e:
        error_msg=f"Fatal error in main loop: {e}"
        logging.error(error_msg)
        send_to_zabbix(error_msg)
        sys.exit(1)


