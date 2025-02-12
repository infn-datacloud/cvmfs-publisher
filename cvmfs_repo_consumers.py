# my_consumers.py
# Author: Francesca Del Corso
# Last update: 11 Feb 2025

import pika
import threading
import time
import ssl
import json
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError, BotoCoreError
import logging
import logging.handlers
import os, sys
from datetime import datetime
import requests

with open("cvmfs_repo_consumers_parameters") as json_data_file:
    data = json.load(json_data_file)

RABBITMQ_HOST               = data["rabbitmq"]['host']
RABBITMQ_PORT               = data["rabbitmq"]['port']
RABBITMQ_USER               = data["rabbitmq"]['admin_user']
RABBITMQ_PASSWORD           = data["rabbitmq"]['admin_password']
RABBITMQ_URL                = data["rabbitmq"]['url']
ACCESS_KEY                  = data["ceph-rgw"]['access_key']
SECRET_KEY                  = data["ceph-rgw"]['secret_key']
ROLE                        = data["ceph-rgw"]['role']
ENDPOINT                    = data["ceph-rgw"]['url']
REGION                      = data["ceph-rgw"]['region']
ca_cert                     = data["ssl"]['ca_cert']
client_cert                 = data["ssl"]['client_cert']
client_key                  = data["ssl"]['client_key']

WDIR_PATH   = "/home/ubuntu/my_consumers/"
SSL_CA_CERT = WDIR_PATH + ca_cert
SSL_CL_CERT = WDIR_PATH + client_cert
SSL_CL_KEY  = WDIR_PATH + client_key

prefetch_count=10
RABBITMQ_EXCLUDED_QUEUES=['cvmfs_reply','cvmfs','publisher','datacloud','delcorso', 'fanzago','gmalatesta','sgaravat', 'spiga','trace']

# SSL context for secure connection
def create_ssl_context():
    context = ssl.create_default_context(cafile=SSL_CA_CERT)
    context.load_cert_chain(certfile=SSL_CL_CERT, keyfile=SSL_CL_KEY)
    context.check_hostname = False        # required check on Linux
    context.verify_mode = ssl.CERT_NONE   # required check on Linux
    return context

# S3 client setup
def S3_client_setup():
    sts_client = boto3.client(
        'sts',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url= ENDPOINT,
        region_name = REGION
        )

    response = sts_client.assume_role(
        RoleArn=f'arn:aws:iam:::role/{ROLE}',
        RoleSessionName='Bob',
        DurationSeconds=3600
        )

    s3_client = boto3.client(
        's3',
        aws_access_key_id = response['Credentials']['AccessKeyId'],
        aws_secret_access_key = response['Credentials']['SecretAccessKey'],
        aws_session_token = response['Credentials']['SessionToken'],
        endpoint_url=ENDPOINT,
        region_name=REGION
        )

    return s3_client


def process_messages(message, queue):
    msg=json.loads(message.decode("utf-8")) 
    try:
        Bucket = msg['Records'][0]['s3']['bucket']['name']   # Bucket=repo01
        Key = msg['Records'][0]['s3']['object']['key']       # key=cvmfs/netCDF-92
        dir_file, file = os.path.split(Key)                  # dir_file=cvmfs, file=netCDF-92
        Operation = msg['Records'][0]['eventName']           # Operation=ObjectCreated:Put ==> download
        print(f"Operation: {Operation}, Bucket: {Bucket}, Key: {Key}")
        logging.info(f"Operation: {Operation}, Bucket: {Bucket}, Key: {Key}")
           
        Filename="/data/cvmfs/" + Bucket + ".infn.it" + dir_file[5:] + "/" + file
        Filename_path = os.path.dirname(Filename)            

        # Create the directory for the file path (ignoring the file itself if present), even with delete operation
        if not os.path.exists(Filename_path):
                os.makedirs(Filename_path)
                print(f"Directory {Filename_path} created successfully.")
                logging.info(f"Directory {Filename_path} created successfully.")
                dir_to_delete = os.path.join(Filename_path, 'to_delete')
                dir_to_extract= os.path.join(Filename_path, 'to_extract')
                if not os.path.exists(dir_to_delete):
                    os.makedirs(dir_to_delete)
                    print(f"Directory {dir_to_delete} created successfully.")
                    logging.info(f"Directory {dir_to_delete} created successfully.")
                if not os.path.exists(dir_to_extract):
                    os.makedirs(dir_to_extract)
                    print(f"Directory {dir_to_extract} created successfully.")
                    logging.info(f"Directory {dir_to_extract} created successfully.")

        # UPLOAD files and .tar files
        if ("ObjectCreated" in Operation):
             if file.endswith('.tar'):       # case upload file.TAR
                 Filename_to_extract="/data/cvmfs/" + Bucket + ".infn.it" + "/to_extract" + dir_file[5:] + "/" + file
                 ris=download_from_s3(Bucket, Key, Filename_to_extract)
             else: # case upload file not .tar
                 ris=download_from_s3(Bucket, Key, Filename)
       

        else:
             # DELETE operation
             if ("ObjectRemoved" in Operation):
                # The file to be removed from CVMFS repo is written in a .txt file in to_delete/ folder
                file_to_be_removed= "/cvmfs/" + Bucket + ".infn.it" + dir_file[5:] + "/" + file
                to_delete_file = Filename_path + "/to_delete/" + Bucket + "-infn-it.txt"
                with open(to_delete_file, "a") as f:
                    f.write(file_to_be_removed + "\n")
                ris = True
             else:
                print("Operation not supported.")
                logging.info("Operation not supported.")
                ris = False


    except Exception as e:
            print(f"Failed to process message: {str(e)}")
            logging.info(f"Failed to process message: {str(e)}")    
            ris= False

    return ris


def download_from_s3(bucket, key, Filename):    
    s3=S3_client_setup()
    try:        
        s3.download_file(bucket, key, Filename)
        print(f"Successfully downloaded {key} to {Filename}.")
        logging.info(f"Successfully downloaded {key} to {Filename}.")
        return True

    except FileNotFoundError:
        print(f'FileNotFound ERROR. Filename={Filename}, bucket={bucket}, key={key}.')
        logging.info(f'FileNotFound ERROR. Filename={Filename}, bucket={bucket}, key={key}.')
        # controllo che non esista il file e se non esiste provo a scaricarlo in /tmp
        if not os.path.exists(os.path.dirname(download_path)):
           print(f"Download path not found. Defaulting to /tmp.")
           download_path = os.path.join('/tmp', os.path.basename(Filename))
           try:
               s3.download_file(bucket, key, download_path)
               print(f"File {key} downloaded successfully to {download_path}")
               return True
           except ClientError as e:
               print(f'File {key} NOT downloaded successfully to {download_path}')
               logging.info(f'File {key} downloaded successfully to {download_path}')
               return False
        else:
            return False

    except NoCredentialsError:
        print('Credentials not available.')
        logging.info('Credentials not available.')
        return False

    except PartialCredentialsError:
        print('Incomplete credentials provided.')
        logging.info('Incomplete credentials provided.')   
        return False

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f'The object {key} does not exist in the bucket {bucket}. Not downloaded.')
            logging.info(f'The object {key} does not exist in the bucket {bucket}. Not downloaded.')
            # Downloading a non existing S3 file is not considered an error      
            return True
        else:
            print(f'Client error: {e}')
            logging.info(f'Client error: {e}')   
            return False

    except BotoCoreError as e:
        print(f'BotoCoreError occurred: {e}')
        logging.info(f'BotoCoreError occurred: {e}') 
        return False

    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        logging.info(f'An unexpected error occurred: {e}')
        return False

# RabbitMQ connection
def connect_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    ssl_context = create_ssl_context()
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,port=RABBITMQ_PORT,credentials=credentials,ssl_options=pika.SSLOptions(context=ssl_context), heartbeat=3600, blocked_connection_timeout=300,retry_delay=5, connection_attempts=3)
    return pika.BlockingConnection(parameters)



# Messages processing function 
def callback(ch, method, properties, body):
    ris=process_messages(body, method.routing_key)
    if ris == True:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Acknowledged message for {method.routing_key}.")
        logging.info(f"Acknowledged message for {method.routing_key}.")
    else:
        print(f"Some operations failed. Not acknowledging message for {method.routing_key}.")
        logging.info(f"Some operations failed. Not acknowledging message for {method.routing_key}.")



# to manage a queue with a single thread
class QueueWorker(threading.Thread):
    def __init__(self, queue_name):
        super().__init__()
        self.queue_name = queue_name
        self.running = True

    def run(self):
        while self.running:
            try:
                connection = connect_rabbitmq()
                channel = connection.channel()
                channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-type':'quorum'})
                channel.basic_qos(prefetch_count=prefetch_count)
                channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
                print(f"[✓] Listening on {self.queue_name}")
                channel.start_consuming()
            except Exception as e:
                print(f"[!] Error in {self.queue_name}: {e}. Restarting...")
                time.sleep(5)  # wait before trying again

    def stop(self):
        self.running = False

# To monitor threads and restart them if necessary
class ThreadMonitor:
    def __init__(self, queues):
        self.queues = queues
        self.threads = {}

    def start_threads(self):
        for queue in self.queues:
            self.start_thread(queue)

    def start_thread(self, queue):
        if queue not in self.threads or not self.threads[queue].is_alive():
            print(f"[+] Starting worker for {queue}")
            worker = QueueWorker(queue)
            worker.start()
            self.threads[queue] = worker

    def monitor_threads(self):
        while True:
            for queue in self.queue
                self.start_thread(queue)
            time.sleep(10)  # wait before trying again


# Generate log function
def log_generation():
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"log/my_consumers_{date_stamp}.log"
    logging.basicConfig(
     level=logging.INFO,                    # Set the logging level: INFO, ERROR, DEBUG
     filename=log_filename,                 # Specify log file name
     filemode='a',                          # Append to the file if it exists
     format='%(asctime)s - %(levelname)s - %(message)s'
    )


# Getting RabbitMQ queues
def list_queues():
    try:
        url = f'{RABBITMQ_URL}/api/queues'
        create_ssl_context()
        requests.packages.urllib3.disable_warnings()  # To disable warnings in requests vendor urllib3
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD), verify=False)
        
        if response.status_code == 200:
            queues = response.json()
            filtered_queues = [queue['name'] for queue in queues if queue['name'] not in RABBITMQ_EXCLUDED_QUEUES and 'amq.gen' not in  queue['name']]
            return filtered_queues
        else:
            print(f"Failed to fetch queues. Status code: {response.status_code}")
            logging.info(f"Failed to fetch queues. Status code: {response.status_code}")
            return []

    except requests.exceptions.RequestException as e:
       print(f"Error while connecting to RabbitMQ API: {e}")
       return []


def main():
    
    log_generation()        
    QUEUES=list_queues()    # to be done periodically, e.g. every 30 minutes
    monitor = ThreadMonitor(QUEUES)
    monitor.start_threads()
    monitor.monitor_threads()


if __name__ == "__main__":
    
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


