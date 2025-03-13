# Prove_di_Carico_S3_bucket.py
# Last update: 15 Jan 2025
# @ This script uploads or deletes files to S3 in the S3 cvmfs user bucket. BEFORE RUNNING THIS SCRIPT, OIDC-AGENT PID AND SOCK must be loaded. 
# S3 access occurs thanks to the IAM TOKENS obtained with oidc-agent and IAM https://iam.cloud.infn.it/.
# The script does not handle NETWORK PROBLEMS that CAUSE SENDING FLOW INTERRUPTION. WHEN CONNECTIVITY RETURNS, THE SCRIPT STARTS FROM THE FIRST REPO.

import boto3
import os 
import random
import logging
import subprocess
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


bucket_list=['repo01','repo02','repo03','repo04','repo05','repo06','repo07','repo08','repo09','repo10','repo11','repo12','repo13','repo14','repo15','repo16', 'repo17','repo18','repo19','repo20','repo21','repo22','repo23','repo24','repo25','repo26','repo27','repo28','repo29','repo30']

# On Windows
path =        "Prove_di_carico\\sw\\high\\100\\"
output_file = "Prove_di_carico\\Prove_di_Carico.txt"


def get_oidc_token():
    command = r'C:\Program Files\oidc-agent\oidc-agent\oidc-token.exe --aud=object delcorso'
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"An error occurred getting a token IAM: {e}")
        return None
    
def get_s3_client(TOKEN_IAM):
        if TOKEN_IAM:
                print(f"Token IAM updated: {TOKEN_IAM}")
                
                sts_client = boto3.client('sts',
                                endpoint_url="https://rgw.cloud.infn.it:443",
                                region_name='default'
                                )
                response = sts_client.assume_role_with_web_identity(
                                RoleArn="arn:aws:iam:::role/IAMaccess",
                                RoleSessionName='Bob',
                                DurationSeconds=3600,
                                WebIdentityToken = TOKEN_IAM
                                )

                s3_client = boto3.client('s3',
                                aws_access_key_id = response['Credentials']['AccessKeyId'],
                                aws_secret_access_key = response['Credentials']['SecretAccessKey'],
                                aws_session_token = response['Credentials']['SessionToken'],
                                endpoint_url="https://rgw.cloud.infn.it:443",
                                region_name='default',
                                )

        else:
                print("Failed to update token.")


        return s3_client

# To upload 1 file choosen at random
def upload_file(path):
        file = random.choice([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
        print(file + " selected to be uploaded.")
        return file

# DELETE bucket objects
def delete_bucket_list(bucket_objects):
        for object in bucket_objects:
                        s3_client.delete_object(
                        Bucket = bucket,
                        Key = object
                        )
                        print(f'Bucket {bucket}: {object} successfully deleted.')
                        
# LIST bucket content
def list_bucket_content(bucket):
        bucket_objects =[]
        resp = s3_client.list_objects(Bucket=bucket)
        for content in resp.get('Contents', []):
                bucket_objects.append(content['Key'])     
        return(bucket_objects)


# Main operations
try:
        with open(output_file, 'a') as f1:
                f1.write("TIME\tBucket\tFilename\tUploadTime\n")   

                # GET THE TOKEN AND ACCESS S3
                TOKEN_IAM = get_oidc_token()
                s3_client = get_s3_client(TOKEN_IAM)   
                
                START_TIME=datetime.now()
                for bucket in bucket_list:
                        START_BUCKET_TIME = datetime.now()
                        print(START_BUCKET_TIME)

                        # UPLOAD FILES
                        #FILE = upload_file(path)                                          # UPLOAD di 1 solo file, scelto a caso nella cartella  
                        n_file_uploaded=0
                        for FILE in os.listdir(path):                                      # UPLOAD DI TUTTI i file contenuti nella cartella
                                   file_stats = os.stat(path+FILE)
                                   file_size_mb = file_stats.st_size / (1024 * 1024)          # dim del file in MB
                                   with open(path+FILE, "rb") as f:
                                                   object_name = 'cvmfs/'+ FILE               # è il nome del file che serve per creare l'object dentro S3
                                                   start_upload = datetime.now()
                                                   s3_client.upload_fileobj(f, bucket, object_name,ExtraArgs={'ACL': 'bucket-owner-full-control'})
                                                   end_upload = datetime.now()
                                                   print(f'{bucket} bucket: {FILE} successfully uploaded in {end_upload-start_upload}. Upload time: {start_upload}')
                                                   upload_time=end_upload-start_upload 
                                                   n_file_uploaded += 1
                                                   f1.write(str(start_upload) + "\t" + bucket +"\t" + FILE + "\t" + str(upload_time)+ "\n")   
                        print(f'{n_file_uploaded} files successfully uploaded for bucket {bucket} in {end_upload-START_BUCKET_TIME}.')
                              

                        # REMOVE FILES 
                        # start_rm = datetime.now()
                        # bucket_objects = list_bucket_content(bucket)
                        # bucket_objects_to_remove  = bucket_objects                        # quando voglio eliminare tutti i msg contenuti nel bucket su S3
                        # # bucket_objects_to_remove =[random.choice(bucket_objects)]       # quando voglio eliminarne 1 a caso
                        # delete_bucket_list(bucket_objects_to_remove)
                        # end = datetime.now()
                        # n_file_removed = len(bucket_objects_to_remove)
                        # print(f'{n_file_removed} files successfully deleted for bucket {bucket} in {end-start_rm}. Time: {start_rm}')


                        # RINNOVO (EVENTUALE) DEL TOKEN
                        END_TIME = datetime.now()
                        START_BUCKET_TIME_MINUTE = int(str(END_TIME - START_TIME).split(':')[1])
                        
                        print("Renew the token?", START_BUCKET_TIME_MINUTE, "minutes are passed.")
                        if (START_BUCKET_TIME_MINUTE > 20):             # Se sono passati almeno 20 minuti da quando è stato preso un token, riprendine uno nuovo
                                TOKEN_IAM = get_oidc_token()
                                print("YES, renew the token.")
                                START_TIME = END_TIME                   # Aggiorno il timer da cui partire per il successivo aggiornamento token
                                s3_client = get_s3_client(TOKEN_IAM)
                        else:
                                print("NO, don't renew the token.")
                

except NoCredentialsError:
        print('Error: No AWS credentials found.')
except PartialCredentialsError:
        print('Error: Incomplete AWS credentials found.')

except ClientError as e:
        logging.error(e)

except Exception as e:
        print(f'{e}')

