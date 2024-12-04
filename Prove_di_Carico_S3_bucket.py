# Prove_di_Carico_S3_bucket.py
# Author: Francesca Del Corso
# Last update: 22 Nov. 2024 

import boto3
import os 
import random
import logging
import subprocess
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


bucket_list=['repo01','repo02','repo03','repo04','repo05','repo06','repo07','repo08','repo09','repo10','repo11','repo12','repo13','repo14','repo15','repo16','repo17','repo18','repo19','repo20']  
path = "Prove_di_carico\\sw\\small\\100\\"
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
                bucket_objects.append(content['Key'])     # repo bucket list objects
        return(bucket_objects)

while True:
 try:
        # For Debugging Purposes 
        #boto3.set_stream_logger(name='botocore')

        with open(output_file, 'a') as f1:
                f1.write("TIME\tBucket\tFilename\tUploadTime\n")   

                # GET THE TOKEN AND ACCESS S3
                TOKEN_IAM = get_oidc_token()
                s3_client = get_s3_client(TOKEN_IAM)   
                
                START_TIME=datetime.now()
                for bucket in bucket_list:
                        START_BUCKET_TIME = datetime.now()
                        print(START_BUCKET_TIME)
                        n_file_uploaded=0
                        #FILE = upload_file(path)                                     # UPLOAD 1 file, choosen at random
                        for FILE in os.listdir(path):                                 # UPLOAD files 
                                 file_stats = os.stat(path+FILE)
                                 file_size_mb = file_stats.st_size / (1024 * 1024)       # file dim (in MB)
                                 with open(path+FILE, "rb") as f:
                                                 object_name = 'cvmfs/'+ FILE               # file name to create S3 object
                                                 start_upload = datetime.now()
                                                 s3_client.upload_fileobj(f, bucket, object_name,ExtraArgs={'ACL': 'bucket-owner-full-control'})
                                                 end_upload = datetime.now()
                                                 print(f'{bucket} bucket: {FILE} successfully uploaded in {end_upload-start_upload}. Upload time: {start_upload}')
                                                 upload_time=end_upload-start_upload 
                                                 n_file_uploaded += 1
                                                 f1.write(str(start_upload) + "\t" + bucket +"\t" + FILE + "\t" + str(upload_time)+ "\n")   
                        print(f'{n_file_uploaded} files successfully uploaded for bucket {bucket} in {end_upload-START_BUCKET_TIME}.')
                              
                        # FILES REMOVE
                        # start_rm = datetime.now()
                        # bucket_objects = list_bucket_content(bucket)
                        # bucket_objects_to_remove  = bucket_objects                   # to delete all files
                        # bucket_objects_to_remove =[random.choice(bucket_objects)]    # to delete 1 file choosen at random                        # delete_bucket_list(bucket_objects_to_remove)
                        # end = datetime.now()
                        # n_file_removed = len(bucket_objects_to_remove)
                        # print(f'{n_file_removed} files successfully deleted for bucket {bucket} in {end-start_rm}. Time: {start_rm}')

                        # TOKEN renew
                        END_TIME = datetime.now()
                        START_BUCKET_TIME_MINUTE = int(str(END_TIME - START_TIME).split(':')[1])
                        
                        print("Renew the token?", START_BUCKET_TIME_MINUTE, "minutes are passed.")
                        if (START_BUCKET_TIME_MINUTE > 20):             # if 20 minutes are passed, renew token
                                TOKEN_IAM = get_oidc_token()
                                print("YES, renew the token.")
                                START_TIME = END_TIME                   # timer update to start counting for next token update
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

