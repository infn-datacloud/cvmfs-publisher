# Author: Francesca Del Corso
# Last update: 05 Nov. 2024 - versione SOLO download e cancellazione

import pika
import asyncio
import ssl
import json
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError, BotoCoreError
import os,sys
import subprocess
from datetime import datetime
import requests
import logging
import logging.handlers

with open("my_consumers_parameters.json") as json_data_file:
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

ack_threshold = 10
RABBITMQ_EXCLUDED_QUEUES=['bucketupdate','cvmfs_reply','cvmfs','publisher','datacloud']

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

# Filename = path + nome del file che verrà dato alla key scaricata
async def download_from_s3(bucket, key, Filename):
    s3=S3_client_setup()
    try:
        start_download = datetime.now()
        s3.download_file(bucket, key, Filename)
        end_download = datetime.now()
        print(f"Successfully downloaded {key} to {Filename} in {end_download - start_download}")
        await asyncio.sleep(0.1)
        return True

    except FileNotFoundError:
        print(f'The specified download path is not found.')
        return False
    except NoCredentialsError:
        print('Credentials not available.')
        return False
    except PartialCredentialsError:
        print('Incomplete credentials provided.')
        return False
    except ClientError as e:
        # You can further refine the exception handling based on the error code
        if e.response['Error']['Code'] == '404':
            print(f'The object {key} does not exist in the bucket {bucket}. Not downloaded.')
            # Non posso fare il download di un file che su s3 non esiste, e poiché la repo cvmfs deve essere sincronizzata con s3, questo caso non viene considerato errore
            return True
        else:
            print(f'Client error: {e}')
            return False
    except BotoCoreError as e:
        print(f'BotoCoreError occurred: {e}')
        return False
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        return False

async def delete_from_s3(Filename):
    try:
        start_delete = datetime.now()
        # operazione di ricerca e cancellazione file
        if os.path.isfile(Filename):
           os.remove(Filename)
           end_delete = datetime.now()
           print(f"File '{Filename}' successfully deleted in {end_delete - start_delete}.")
        else:
           # cancellare un file che non esiste non costituisce errore
           print(f"File '{Filename}' was not found.")
        await asyncio.sleep(0.1)
        return True

    except Exception as e:
        print(f'An error occurred while trying to delete the file: {e}')
        return False


def cvmfs_transaction(queue):
    try:
        repo=queue+".infn.it"
        # Verifica se la repo è in transaction
        # sudo cvmfs_server list | grep repo02.infn.it deve contenere la stringa "in transaction"
        resp=subprocess.run(["sudo", "cvmfs_server", "list"], check=True, capture_output=True)
        resp1 = subprocess.run(["grep", repo], input=resp.stdout, capture_output=True)
        if "transaction" in resp1.stdout.decode('utf-8'):
            print(f"The repository '{repo}' is in a transaction.")
        else:
            subprocess.run(["sudo", "cvmfs_server", "transaction",repo], check=True)
            print("cvmfs_server transaction successfull.")
    except subprocess.CalledProcessError as e:
        print(f"Error in cvmfs-server transaction operation: {e}")
        cvmfs_abort(queue)

def cvmfs_publish(queue):
    try:
        queue_name=queue+".infn.it"
        subprocess.run(["sudo", "cvmfs_server", "publish",queue_name], check=True)
        print("cvmfs_server publish successfull.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error in cvmfs-server publish operation: {e}")
        return False

#   try:
#        # Execute the cvmfs_server publish command
#        result = subprocess.run(
#            ['cvmfs_server', 'publish', repo_name],
#            check=True,
#            capture_output=True,
#            text=True
#        )

#        print("Command executed successfully:")
#        print(result.stdout)
#
#    except subprocess.CalledProcessError as e:
#        print(f"Command failed with exit code {e.returncode}: {e.stderr.strip()}")
#        cvmfs_abort(queue)
#    except PermissionError:
#        print(f"Permission denied when trying to execute cvmfs_server.")
#        cvmfs_abort(queue)
#    except FileNotFoundError:
#        print("The cvmfs_server command was not found. Please ensure it is installed.")
#    except OSError as e:
#        print(f"OS error occurred: {e}")
#    except Exception as e:
#        print(f"An unexpected error occurred: {e}")


def cvmfs_abort(queue):
    try:
        queue_name=queue+".infn.it"
        subprocess.run(["sudo", "cvmfs_server", "abort", "-f", queue_name], check=True)
        print("cvmfs_server abort successfull.")
    except subprocess.CalledProcessError as e:
        print(f"Error in cvmfs-server abort operation: {e}")


# Async function to process a batch of messages
async def process_messages(channel, queue, messages, methods):
    print(f"Processing {len(messages)} messages from {queue}:")
    tasks = [] #  boolean vector
    
    # CVMFS transaction 
    #cvmfs_transaction(queue)
    
        # Procedo con il download degli N messaggi
    for message in messages:
        msg=json.loads(message.decode("utf-8")) # La stringa message per essere trattata come dizionario va passata alla funzione json.loads
        try:
            Bucket = msg['Records'][0]['s3']['bucket']['name']   # Bucket=repo01
            Key = msg['Records'][0]['s3']['object']['key']       # key=cvmfs/netCDF-92
            dir_file, file = os.path.split(Key)                  # dir_file=cvmfs, file=netCDF-92
            Operation = msg['Records'][0]['eventName']           # Operation=ObjectCreated:Put ==> download
            print(f"Operation: {Operation}, Bucket: {Bucket}, Key: {Key}")
            
            #Filename="/cvmfs/" + Bucket + ".infn.it" + dir_file[5:] + "/" + file    
            Filename="/home/ubuntu/my_consumers/cvmfs/" + Bucket + ".infn.it" + dir_file[5:] + "/" + file
            Filename_path = os.path.dirname(Filename)

            # Create the directory for the file path (ignoring the file itself if present), even with delete operation
            if not os.path.exists(Filename_path):
                   os.makedirs(Filename_path)
                   print(f"Directory {Filename_path} created successfully.")

            # DOWNLOAD
            if ("ObjectCreated" in Operation):
               tasks.append(download_from_s3(Bucket, Key, Filename))
            else:
                if ("ObjectRemoved" in Operation):
                   # DELETE
                   tasks.append(delete_from_s3(Filename))
                else:
                   print("Operation not supported.")

        except Exception as e:
            print(f"Failed to process message: {str(e)}")
     
    
    results = await asyncio.gather(*tasks)

    #if (all(results) and cvmfs_publish(queue) == True):
    if all(results):
             # download or delete operations successfull acknowledge the bunch of messages
             channel.basic_ack(delivery_tag=methods[-1].delivery_tag, multiple=True)
             print(f"Acknowledged {len(messages)} messages from {queue}.")
             messages.clear()
    else:
        print(f"Some operations failed. Not acknowledging {len(messages)} of messages from {queue}.")
        #cvmfs_abort(queue) 



# Async function to consume messages from a specific queue
async def rabbitmq_consumer(queue):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    ssl_context = create_ssl_context()
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,port=RABBITMQ_PORT,credentials=credentials,ssl_options=pika.SSLOptions(context=ssl_context), heartbeat=3600, blocked_connection_timeout=300,retry_delay=5, connection_attempts=3)
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare queue 
    channel.queue_declare(queue=queue, durable=True, arguments={'x-queue-type':'quorum'})
    
    channel.basic_qos(prefetch_count=ack_threshold) 

    message_buffer = []  # Buffer for storing methods
    messages = []        # Buffer for storing messages until ACK_THRESHOLD is met

    def callback(ch, method, properties, body):

        message_buffer.append(method)      # Store delivery tag for ACK
        messages.append(body)              # Store messages 
        print("C")

        if (len(message_buffer) >= ack_threshold):  # Il caso coda vuota è trattato andando a controllare message_buffeer 
            print("D")
            asyncio.create_task(process_messages(ch, queue, messages, message_buffer.copy()))
            message_buffer.clear()

    print(f"Waiting for messages from {queue}...")
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)

    try:
        while True:
            print("E")
            connection.process_data_events(time_limit=1)
            # If the queue is empty but messages are in the buffer
            if message_buffer:
                print("F")
                asyncio.create_task(process_messages(channel, queue, messages, message_buffer.copy()))
                message_buffer.clear()
            await asyncio.sleep(0.1)  # to control the frequency of queue checks

    except asyncio.CancelledError:
        print(f"Consumer for {queue} stopped!")
        connection.close()

def list_queues():
    try:
        url = f'{RABBITMQ_URL}/api/queues'
        #context=create_ssl_context()
        create_ssl_context()
        requests.packages.urllib3.disable_warnings()  # To disable warnings in requests vendor urllib3
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD), verify=False)
        
        if response.status_code == 200:
            queues = response.json()
            filtered_queues = [queue['name'] for queue in queues if queue['name'] not in RABBITMQ_EXCLUDED_QUEUES and 'amq.gen' not in  queue['name']]
            #print(f"List of filtered Queue Names: {filtered_queues}")
            return filtered_queues
        else:
            print(f"Failed to fetch queues. Status code: {response.status_code}")
            return []

    except requests.exceptions.RequestException as e:
       print(f"Error while connecting to RabbitMQ API: {e}")
       return []



async def main():
    # Generate log file with current date
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"log/my_consumers_{date_stamp}.log"

    # Configure logging
    logging.basicConfig(
     level=logging.DEBUG,                   # Set the logging level
     filename=log_filename,                 # Specify the log file name
     filemode='a',                          # Append to the file if it exists
     format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # List all RabbitMQ eligible queues
    queues=list_queues()
    
    # Start multiple consumers, one per queue
    tasks = []
    for queue in queues:
        task = asyncio.create_task(rabbitmq_consumer(queue))
        tasks.append(task)
    
    # Wait for all consumer tasks to run indefinitely
    await asyncio.gather(*tasks)



if __name__ == "__main__":

    try:
        # Run the event loop
        asyncio.run(main())

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)







