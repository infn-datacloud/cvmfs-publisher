# Prove_di_Carico_S3_bucket.py

import boto3
import os 
import random
import logging
import subprocess
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


bucket_list=['repo01','repo02','repo03','repo04','repo05','repo06','repo07','repo08','repo09','repo10','repo11','repo12','repo13','repo14','repo15', 'repo16','repo17','repo18','repo19','repo20']
path = "Prove_di_carico\\sw\\"
output_file = "Prove_di_carico\\Prove_di_Carico.txt"



def get_oidc_token():
    command = r'C:\Program Files\oidc-agent\oidc-agent\oidc-token.exe --aud=object delcorso'
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
        return None
    
def get_S3_client(TOKEN_IAM):
        if TOKEN_IAM:
                print(f"Token IAM updated: {TOKEN_IAM}")
        else:
                print("Failed to update token.")

        response = sts_client.assume_role_with_web_identity(
                RoleArn="arn:aws:iam:::role/IAMaccess",
                RoleSessionName='Bob',
                DurationSeconds=3600,
                WebIdentityToken = TOKEN_IAM
                )

        s3client = boto3.client('s3',
                aws_access_key_id = response['Credentials']['AccessKeyId'],
                aws_secret_access_key = response['Credentials']['SecretAccessKey'],
                aws_session_token = response['Credentials']['SessionToken'],
                endpoint_url="https://rgw.cloud.infn.it:443",
                region_name='default',
                )
        return(s3client)

# To upload 1 file choosen at random
def upload_file(path):
        file = random.choice([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
        print(file + " selected to be uploaded.")
        return file

# DELETE bucket objects
def delete_bucket_list(bucket_objects):
        for object in bucket_objects:
                        s3client.delete_object(
                        Bucket = bucket,
                        Key = object
                        )
                        print(f'Bucket {bucket}: {object} successfully deleted.')
                        
# LIST bucket content
def list_bucket_content(bucket):
        bucket_objects =[]
        resp = s3client.list_objects(Bucket=bucket)
        for content in resp.get('Contents', []):
                bucket_objects.append(content['Key'])     # elenco oggetti presenti nel repo bucket
        return(bucket_objects)

# For Debugging Purposes 
#boto3.set_stream_logger(name='botocore')

# Secure Token Service initialization
sts_client = boto3.client('sts',
        endpoint_url="https://rgw.cloud.infn.it:443",
        region_name='default'
        )  

try:
        with open(output_file, 'a') as f1:
                f1.write("TIME\tBucket\tFilename\tUploadTime\n")   

                # GET THE TOKEN AND S3 ACCESS
                TOKEN_IAM = get_oidc_token()
                s3client = get_S3_client(TOKEN_IAM)    
                
                for bucket in bucket_list:
                        START_BUCKET_TIME = datetime.now()
                        print(START_BUCKET_TIME)
                        #FILE = upload_file(path)                                          # UPLOAD di 1 solo file, scelto a caso nella cartella
                        #UPLOAD di più file (con tempi di esecuzione)
                        for FILE in os.listdir(path):                                      # UPLOAD dei file contenuti nella cartella
                                                  file_stats = os.stat(path+FILE)
                                                  file_size_mb = file_stats.st_size / (1024 * 1024)       # dim del file in MB
                                                  with open(path+FILE, "rb") as f:
                                                               object_name = 'cvmfs/'+ FILE               # è il nome del file che serve per creare l'object dentro S3
                                                               start_upload = datetime.now()
                                                               s3client.upload_fileobj(f, bucket, object_name,ExtraArgs={'ACL': 'bucket-owner-full-control'})
                                                               # Aggiunto ExtraArgs={'ACL': 'bucket-owner-full-control'} in data 13/11/2024 per ovviare all'errore: 
                                                               # ERROR:root:An error occurred (AccessDenied) when calling the CompleteMultipartUpload operation: Unknown
                                                               # che si verifica in maniera random dopo un pò che il client usa il multipartUpload con file grossi
                                                               end_upload = datetime.now()
                                                               print(f'{bucket} bucket: {FILE} successfully uploaded in {end_upload-start_upload}. Upload time: {start_upload}')
                                                               upload_time=end_upload-start_upload 
                                                               f1.write(str(start_upload) + "\t" + bucket +"\t" + FILE + "\t" + str(upload_time)+ "\n")   

                        # REMOVE FILES 
                        # start_rm = datetime.now()
                        # bucket_objects = list_bucket_content(bucket)
                        # bucket_objects_to_remove  = bucket_objects                    # quando voglio eliminarli tutti
                        # # # # # # # # # # #bucket_objects_to_remove =[random.choice(bucket_objects)]       # quando voglio eliminarne 1 a caso
                        # delete_bucket_list(bucket_objects_to_remove)
                        # end = datetime.now()
                        # print(f'{bucket_objects_to_remove} files successfully deleted for bucket {bucket} in {end-start_rm}. Time: {start_rm}')


                        # RINNOVO (EVENTUALE) DEL TOKEN
                        END_BUCKET_TIME = datetime.now()
                        START_BUCKET_TIME_MINUTE = int(str(END_BUCKET_TIME - START_BUCKET_TIME).split(':')[1])
                        print("Sono passati ", START_BUCKET_TIME_MINUTE, " minuti. Renew token?")
                        if (START_BUCKET_TIME_MINUTE > 20):             # Se sono passati più di 20 minuti da quando è stato preso un token, riprendine uno nuovo
                                TOKEN_IAM = get_oidc_token()
                                print("Renew token!")
                                START_BUCKET_TIME = END_BUCKET_TIME
                                s3client = get_S3_client(TOKEN_IAM)
                        else:
                                print("No")
                
                        print(END_BUCKET_TIME)

except NoCredentialsError:
        print('Error: No AWS credentials found.')
except PartialCredentialsError:
        print('Error: Incomplete AWS credentials found.')

except ClientError as e:
        logging.error(e)

except Exception as e:
        print(f'{e}')

