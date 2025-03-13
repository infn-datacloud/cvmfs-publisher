import boto3
import pika
import pika.exceptions
import hvac
import ssl
import argparse
import subprocess
import logging
import logging.handlers
from pathlib import Path
import shutil
from consumer_users_queues.conf.settings import get_settings


def create_topic(repo):

    s = get_settings()
    repo = repo.split('.')[0]
    try:

        sns_client = boto3.client('sns',
        aws_access_key_id = s.ACCESS_KEY,
        aws_secret_access_key = s.SECRET_KEY,
        endpoint_url= s.ENDPOINT,
        region_name='default',
        )
        OPERATOR_LOGGER.info('CREATING TOPIC...')
        
        attributes = {'push-endpoint' : f'amqps://{s.RMQ_USERNAME}:{s.RMQ_PASSWORD}@{s.RMQ_HOST}:{s.RMQ_PORT}' , 'amqp-exchange': s.RMQ_EXCHANGE, 'amqp-ack-level': 'broker', 'verify-ssl':'false' , 'use-ssl' : 'true' , 'persistent' : 'true'}
        resp = sns_client.create_topic(Name= repo,
                                    Attributes=attributes)
        topic_arn = resp["TopicArn"]
        OPERATOR_LOGGER.info(f'Topic created for repo {repo}, topic_arn = {topic_arn}')

    except Exception as ex:
        OPERATOR_LOGGER.error(f'error: {ex}')

    return True


def delete_topic(s,repo):

    try:

        sns_client = boto3.client('sns',
        aws_access_key_id = s.ACCESS_KEY,
        aws_secret_access_key = s.SECRET_KEY,
        endpoint_url= s.ENDPOINT,
        region_name='default',
        )
        repo = repo.split('.')[0]
        arn = f'arn:aws:sns:bbrgwzg::{repo}'
        resp = sns_client.delete_topic(
            TopicArn=arn)       
        OPERATOR_LOGGER.info(f'Topic deleted for repo {repo}')
                                 
    except Exception as ex:
        OPERATOR_LOGGER.error(f'error: {ex}')
    
    return True

def create_queue(channel, repo):
    
    s = get_settings()
    repo = repo.split('.')[0]
    try:
        channel.queue_declare(queue=repo, durable=True, arguments={"x-queue-type": "quorum"}, exclusive=False)
        channel.queue_bind(
        exchange = s.RMQ_EXCHANGE,
        queue = repo,
        routing_key= repo,
        )
        OPERATOR_LOGGER.info(f'Queue created for repo {repo}')

    except pika.exceptions.ConnectionClosed as ex:
        OPERATOR_LOGGER.warning(f'RabbitMQ client unreachable or dead: {ex}')
    except pika.exceptions.StreamLostError as ex:
        OPERATOR_LOGGER.warning(f'RabbitMQ lost connection: {ex}')
    except Exception as ex:
        OPERATOR_LOGGER.warning(f'strange error: {ex}')

    return True


def get_repo_keys(msg):
    '''Retrive CVMFS repo keys from vault '''

    s = get_settings()
    subject = msg.split(',')[1]
    repository_name=msg.split(',')[2]
    type_repo = msg.split(',')[3]
    try:
        hvacclient = hvac.Client(s.V_URL)
        hvacclient.auth.approle.login(
        role_id=s.V_ROLEID,
        secret_id=s.V_SECRETID,
        )
        SYSLOG_LOGGER.info(f"VAULT client authenticated: {hvacclient.is_authenticated()}")
        SYSLOG_LOGGER.info(f'Vault initialize status: {hvacclient.sys.is_initialized()}')
        SYSLOG_LOGGER.info(f"Vault is sealed: {hvacclient.sys.is_sealed()}")
        OPERATOR_LOGGER.info(f"VAULT client authenticated: {hvacclient.is_authenticated()}")
        OPERATOR_LOGGER.info(f'Vault initialize status: {hvacclient.sys.is_initialized()}')
        OPERATOR_LOGGER.info(f"Vault is sealed: {hvacclient.sys.is_sealed()}")
        if type_repo == 'P':    
            PATH = "secrets/data/"+subject+"/cvmfs_keys/"+repository_name+"/"
        else:
            PATH = "secrets/data/groups/"+repository_name.split('.')[0]+"/cvmfs_keys/"+repository_name+"/"
        read_response = hvacclient.read(path=PATH)
        p = Path('/tmp/') / f'{repository_name}_keys'
        p.mkdir(exist_ok=True)
        with (p / f'{repository_name}.crt').open('w') as f:
            f.write(read_response['data']['data']['certificateKey'])
        with (p / f'{repository_name}.gw').open('w') as f:
            f.write(read_response['data']['data']['gatewayKey'])
        with (p / f'{repository_name}.pub').open('w') as f:
            f.write(read_response['data']['data']['publicKey'])

    except Exception as ex:
        SYSLOG_LOGGER.warning(f'{ex}')
        OPERATOR_LOGGER.warning(f'{ex}')
        return False
    
    return repository_name


def create_repo_publisher(repo_name):
    '''Make a CVMFS repository writable from publisher via gateway'''

    s = get_settings()
    cmd = f'sudo cvmfs_server mkfs -w {s.SERVER_URL}{repo_name} \
    -u gw,/srv/cvmfs/{repo_name}/data/txn,{s.UP_STORAGE} \
    -k /tmp/{repo_name}_keys -o `whoami` {repo_name}'

    # questo va ma lo commento per rimettere in uso le var, poi sarà da cancellare
    #cmd = f'sudo cvmfs_server mkfs -w https://rgw.cloud.infn.it:443/cvmfs/{repo_name} \
    #-u gw,/srv/cvmfs/{repo_name}/data/txn,http://cvmfs.wp6.cloud.infn.it:4929/api/v1 \
    #-k /tmp/{repo_name}_keys -o `whoami` {repo_name}'

    proc = subprocess.run(cmd, capture_output=True,
                              text=True, shell=True, check=False)
    if proc.returncode != 0:
        SYSLOG_LOGGER.error(f'[{repo_name}] - {proc.stderr}')
        OPERATOR_LOGGER.error(f'[{repo_name}] - {proc.stderr}')
        return False
    else:
        SYSLOG_LOGGER.info(f'[{repo_name}] - CVMFS repository created!')
        OPERATOR_LOGGER.info(f'[{repo_name}] - CVMFS repository created!')
    shutil.rmtree(f'/tmp/{repo_name}_keys/')

    return True 


def callback(ch, method, properties, body):
    '''Function called whenever a message from publisher queue is received'''
    # This msg will be sent from the INFN dashboard (and now from the cvmfs_repo_agent.py script on the stratum0) when a user requires a new cvmfs personal/group repository
    message=body.decode("utf-8")
    repo_name = get_repo_keys(message)
    if repo_name is not False:
        res = create_repo_publisher(repo_name)
        if res is not True:
            SYSLOG_LOGGER.warning(f'Cannot create CVMFS repo in publisher: {res}')
            OPERATOR_LOGGER.warning(f'Cannot create CVMFS repo in publisher: {res}')
        else:
            create_t=create_topic(repo_name)
            if create_t is not True:
                SYSLOG_LOGGER.warning(f'Cannot create topic for the CVMFS repo in publisher: {create_t}')
                OPERATOR_LOGGER.warning(f'Cannot create topic for the CVMFS repo in publisher: {create_t}')
            else:
                create_q=create_queue(ch, repo_name)
                if create_q is not True:
                    SYSLOG_LOGGER.warning(f'Cannot create queue for the CVMFS repo in publisher: {create_q}')
                    OPERATOR_LOGGER.warning(f'Cannot create queue for the CVMFS repo in publisher: {create_q}')
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(s):

        try: 
            context = ssl.create_default_context(cafile=s.CA_CERT)
            context.load_cert_chain(certfile=s.CLIENT_CERT, keyfile=s.CLIENT_KEY)
            
            # Establish connection with RabbitMQ server
            credentials = pika.PlainCredentials(s.RMQ_USERNAME,s.RMQ_PASSWORD)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=s.RMQ_HOST,
                                                                           port=s.RMQ_PORT,
                                                                           credentials=credentials,
                                                                           ssl_options=pika.SSLOptions(
                                                                               context, server_hostname=s.RMQ_HOSTNAME)
                                                                           )
                                                                           )
            SYSLOG_LOGGER.info('Connected to RabbitMQ, starting consuming publisher queue...')
            OPERATOR_LOGGER.info('Connected to RabbitMQ, starting consuming publisher queue...')
            channel = connection.channel()
            channel.queue_declare(queue=s.QUEUE_NEW_REPO, durable=True, arguments={"x-queue-type": "quorum"})
            channel.basic_qos(prefetch_count=1)
            
            # Tell RabbitMQ that callback function should receive messages from a specific queue
            channel.basic_consume(queue=s.QUEUE_NEW_REPO,
                                  auto_ack=False,
                                  on_message_callback=callback)
            

            # Enter a never-ending loop that waits for data and runs callbacks whenever necessary
            print(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()

        except pika.exceptions.ConnectionClosed as ex:
            SYSLOG_LOGGER.warning(f'RabbitMQ client unreachable or dead: {ex}')
            OPERATOR_LOGGER.warning(f'RabbitMQ client unreachable or dead: {ex}')
        except pika.exceptions.StreamLostError as ex:
            SYSLOG_LOGGER.warning(f'RabbitMQ lost connection: {ex}')
            OPERATOR_LOGGER.warning(f'RabbitMQ lost connection: {ex}')
        except Exception as ex:
            SYSLOG_LOGGER.warning(f'Strange error: {ex}')
            OPERATOR_LOGGER.warning(f'Strange error: {ex}')



if __name__ == '__main__' :
    
    # Input arguments
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('-cfg', '--conf_file', dest='cfg',
                        help='configuration file')
    PARSER.add_argument('-wdir', '--working_dir', dest='wdir',
                        help='full path to working directory')
    ARGS = PARSER.parse_args()
    # Syslog logging
    SYSLOG_LOGGER = logging.getLogger('syslog')
    FORMATTER = logging.Formatter(' %(levelname)s - %(message)s')
    SYSLOG_HANDLER = logging.handlers.SysLogHandler(address='/dev/log')
    SYSLOG_HANDLER.setFormatter(FORMATTER)
    SYSLOG_LOGGER.addHandler(SYSLOG_HANDLER)
    SYSLOG_LOGGER.setLevel(logging.INFO)
    # Operator logging
    OPERATOR_LOGGER = logging.getLogger('publisher')
    OPERATOR_FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    OPERATOR_LOGGER.setLevel(logging.INFO)
    logg_operator = f'{ARGS.wdir}logs/publisher-consumer.log'
    OPERATOR_HANDLER = logging.FileHandler(logg_operator, mode='w', encoding='utf-8', delay=True)
    OPERATOR_HANDLER.setFormatter(OPERATOR_FORMATTER)
    OPERATOR_LOGGER.addHandler(OPERATOR_HANDLER)
    
    settings = get_settings(ARGS)
    main(settings)
