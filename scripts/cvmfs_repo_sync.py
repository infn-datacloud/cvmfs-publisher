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

import os,sys
import re
import shutil
import subprocess
import time
import logging
import requests
import json
import socket
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

my_cvmfs_path = r"/data/cvmfs"                         # Folder path to monitor
cvmfs_path    = r"/cvmfs/"                             # CVMFS repo base path
TIME_CHECK    = 60                                          # Wait 60 seconds before check again


with open("parameters.json") as json_data_file:
    data = json.load(json_data_file)

ZBX_SERVER                  = data["zabbix"]['server']
ZBX_ITEM_KEY                = data["zabbix"]['item_key2']


# Alerts sent to Zabbix server
def send_to_zabbix(message):
    HOSTNAME =socket.gethostname()
    cmd = f'zabbix_sender -z {ZBX_SERVER} -s {HOSTNAME} -k {ZBX_ITEM_KEY} -o "{message}"'
    try:
        subprocess.run(cmd, shell=True)
    except Exception as e:
        logging.error(f"Zabbix notification failed: {e}")


def setup_logging():
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_file = f"/var/log/publisher/cvmfs_repo_sync-{date_stamp}.log"
    logging.basicConfig(
            level=logging.INFO,                    # Logging level: INFO, ERROR, DEBUG
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[TimedRotatingFileHandler(log_file, when='D', interval=7)]
    )


# This function deletes temporary files copied in the CVMFS repositories. Their filename end with a period followed by 8 characters
def delete_temp_files(directory):
    # re=Regular expression to match filenames
    pattern = re.compile(r".*\.[a-fA-F0-9]{8}$")
    
    # Loop through files in the specified directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        
        # Check if it's a file and matches the pattern
        if os.path.isfile(file_path) and pattern.match(filename):
            try:
                # Delete the file
                logging.info(f"Deleting temparary file {filename} ...")
                print(f"Deleting temparary file {filename} ...")
                os.remove(file_path)
                logging.info(f"{filename} successfully deleted")
                print(f"{filename} successfully deleted")
            except Exception as e:
                error_msg=f"Error deleting {filename}: {e}"
                print(error_msg)
                logging.error(error_msg)
                send_to_zabbix(error_msg)

def cvmfs_transaction(cvmfs_repo):
    try:
        resp=subprocess.run(["sudo", "cvmfs_server", "list"], check=True, capture_output=True)
        resp1 = subprocess.run(["grep", cvmfs_repo], input=resp.stdout, capture_output=True)
        if not "transaction" in resp1.stdout.decode('utf-8'):
            res=subprocess.run(["cvmfs_server", "transaction", cvmfs_repo], capture_output=True,text=True, check=True)
            logging.info(f"{res.stdout}")
            if res.stderr:
                logging.error(f"{res.stderr}")
    
    except subprocess.CalledProcessError as e:
        error_msg=f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction..."
        print(error_msg)
        logging.error(error_msg)
        send_to_zabbix(error_msg)
        # CVMFS ABORT in case of error
        res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True,text=True,check=False)
        logging.info(f"{res.stdout}")
        if res.stderr:
            error_msg=f"CVMFS abort error in cvmfs_repo_sync() function: {res.stderr}"
            print(error_msg)
            logging.error(error_msg)
            send_to_zabbix(error_msg)



def cvmfs_repo_sync():                                  # my_cvmfs_path=/data/cvmfs , cvmfs_path=/cvmfs
    for cvmfs_repo in os.listdir(my_cvmfs_path):        # cvmfs_repo=repo01.infn.it    
        folder_path  = os.path.join(my_cvmfs_path, cvmfs_repo)
        cvmfs_folder = os.path.join(cvmfs_path, cvmfs_repo)
        
        if os.path.isdir(folder_path):
            # Check files in /data/cvmfs/reponame folder and move them into the corresponding CVMFS repository
            files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

            if files:
                sync_time=datetime.now()
                print(f"Syncronization process for CVMFS repository {cvmfs_repo} started at {sync_time}")
                logging.info(f"Syncronization process for CVMFS repository {cvmfs_repo} started.")
                
                try:
                    cvmfs_transaction(cvmfs_repo)
                    
                    # Create directory in the repository if it does not exist
                    if not os.path.exists(cvmfs_folder):
                        logging.info(f"Creating {cvmfs_folder} directory.")
                        os.makedirs(cvmfs_folder)

                    # Copy files in CVMFS dir
                    for file_name in files:
                        file_path = os.path.join(folder_path, file_name)
                        print(f"Copying {file_name} in {cvmfs_folder} ...")
                        logging.info(f"Copying {file_name} in {cvmfs_folder} ...")
                        shutil.copy(file_path, cvmfs_folder)
                    print(f"Copy operation finished. CVMFS publish for {cvmfs_folder} starting ...")
                    logging.info(f"Copy operation finished. CVMFS publish for {cvmfs_folder} starting ...")

                    # Check if temporary or multipart files are present in the directory
                    delete_temp_files(cvmfs_folder)
                    
                    # CVMFS PUBLISH
                    res=subprocess.run(["cvmfs_server", "publish", cvmfs_repo], capture_output=True,text=True,check=True)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                        print(f"CVMFS publish for {cvmfs_folder} error: {res.stderr}")
                        logging.error(f"CVMFS publish for {cvmfs_folder} error: {res.stderr}")
                    else:
                        print(f"CVMFS publish for {cvmfs_repo} successfully completed.")
                        logging.info(f"CVMFS publish for {cvmfs_repo} successfully completed.")
                        
                        # Remove files from /data/cvmfs/repo only after publish successfully finished
                        for file_name in files:
                            os.remove(os.path.join(folder_path, file_name))
                        print(f"Files removed from /data/cvmfs{cvmfs_folder}.")
                        logging.info(f"Files removed from /data/cvmfs{cvmfs_folder}.")

                    sync_end_time=datetime.now()
                    print(f"Syncronization process for the CVMFS repository {cvmfs_repo} successfully completed at {sync_end_time}.")
                    logging.info(f"Syncronization process for the CVMFS repository {cvmfs_repo} successfully completed.")

                except subprocess.CalledProcessError as e:
                    error_msg=f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction..."
                    logging.error(error_msg)
                    send_to_zabbix(error_msg)
                    # CVMFS ABORT in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True,text=True,check=False)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       error_msg=f"CVMFS abort error in cvmfs_repo_sync() function: {res.stderr}"
                       logging.error(error_msg)
                       send_to_zabbix(error_msg)

                except Exception as e:
                    error_msg=f"Unexpected error in cvmfs_repo_sync() function: {e}"
                    logging.error(error_msg)
                    send_to_zabbix(error_msg)
                    # CVMFS ABORT in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True,text=True,check=False)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       error_msg=f"CVMFS abort error in cvmfs_repo_sync() function: {res.stderr}"
                       logging.error(error_msg)
                       send_to_zabbix(error_msg)
           

            # CASE files to DELETE
            files_to_delete = my_cvmfs_path + "/" + cvmfs_repo + "/to_delete/"  + cvmfs_repo.split('.')[0] + "-infn-it.txt"
            if os.path.exists(files_to_delete) and (os.path.getsize(files_to_delete) > 0):  # check file existance and not empty
                print(f"Starting removing files for {cvmfs_repo} CVMFS repository ..")
                logging.info(f"Starting removing files for {cvmfs_repo} CVMFS repository ..")
                delete_cvmfs_files(files_to_delete,cvmfs_repo)


            # CASE .tar files to EXTRACT
            to_extract_path = my_cvmfs_path + "/" + cvmfs_repo + "/to_extract/"
            if os.path.isdir(to_extract_path) and os.listdir(to_extract_path):    
                   tar_files = [f for f in os.listdir(to_extract_path) if os.path.isfile(os.path.join(to_extract_path, f))]
                   cvmfs_extract(cvmfs_repo,tar_files)  


# This function deletes files written in the file to_delete_file and if the operation is successfull, remove the line in the file
def delete_cvmfs_files(to_delete_file,cvmfs_repo):
         
         with open(to_delete_file, "r") as f:
              files = f.readlines()   
         
         remaining_files = []       
         
         try:
            cvmfs_transaction(cvmfs_repo)
            for file in files:
             file_path = file.strip()     
             # CASE deleting .tar file                                              # file_path=/cvmfs/repo01.infn.it/oidc-agent_5.1.0.tar
             if file_path.endswith('.tar'):
                try:
                   # extract the folder name
                   folder_name = os.path.basename(file_path).rsplit('.', 1)[0]      # folder_name=oidc-agent_5.1.0
                   print("Folder name:", folder_name)
                   # define the folder path
                   folder_path = os.path.join(os.path.dirname(file_path), "software", folder_name) # folder_path=/cvmfs/repo01.infn.it/software/oidc-agent_5.1.0/
                   print("Target dir:", folder_path)
                   if os.path.exists(folder_path) and os.path.isdir(folder_path):
                      # The entire folder in /cvmfs/reponame/software/ is to be deleted, not the software dir
                      shutil.rmtree(folder_path)                                  # valuatare cvmfs_server ingest -d
                      print(f"Deleted: {folder_path}.")
                      logging.info(f"Deleted: {folder_path}")
                   else:
                      # Case file not found is not considered as an error, it is only logged in the log file
                      logging.info(f"Folder {folder_path} or filename not found, nothing to delete.")

                except Exception as e:
                   error_msg=f"Unexpected error in delete_cvmfs_files function: {e}"
                   logging.error(error_msg)
                   send_to_zabbix(error_msg)
                   remaining_files.append(file)  # Keep the line if deletion fails

             else:  
             # CASE deleting other types of files, e.g. file_path=/cvmfs/repo01.infn.it/NETCDC01
               try:
                 if os.path.exists(file_path):
                   os.remove(file_path)
                   print(f"Deleted: {file_path}")
                   logging.info(f"Deleted: {file_path}")
                 else:
                   # Case file not found is not considered as an error, it is only logged in the log file
                   print(f"File not found, skipping: {file_path}")
                   logging.info(f"File not found, skipping: {file_path}")

               except Exception as e:
                   error_msg=f"Unexpected error in delete_cvmfs_files function: {e}"
                   logging.error(error_msg)
                   send_to_zabbix(error_msg)
                   remaining_files.append(file)  # Keep the line if deletion fails
        

            # Execute CVMFS publish
            res=subprocess.run(["cvmfs_server", "publish", cvmfs_repo], capture_output=True,text=True,check=True)
            logging.info(f"{res.stdout}")
            if res.stderr:
                error_msg=f"CVMFS publish for {cvmfs_repo} error: {res.stderr}"     
                logging.error(error_msg)
                send_to_zabbix(error_msg)
            else:
                print(f"CVMFS publish for {cvmfs_repo} successfully completed.")
                logging.info(f"CVMFS publish for {cvmfs_repo} successfully completed.")


            # Rewrite the file with remaining paths
            with open(to_delete_file, "w") as f:
                 f.writelines(remaining_files) 

            del_end_time=datetime.now()
            # What is deleted is written before
            print(f"DELETE file operation for {cvmfs_repo} successfully completed at {del_end_time}.")
            logging.info(f"DELETE file operation for {cvmfs_repo} successfully completed.")

         except subprocess.CalledProcessError as e:
                    error_msg=f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction."
                    logging.error(error_msg)
                    send_to_zabbix(error_msg)
                    # Transaction abort in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True,text=True,check=False)
                    print(f"{res.stdout}")
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       error_msg=f"CVMFS abort error in delete_cvmfs_files function: {res.stderr}"
                       logging.error(error_msg)
                       send_to_zabbix(error_msg)

         except Exception as e:
                    error_msg=f"Unexpected error: {e}"
                    logging.error(error_msg)
                    send_to_zabbix(error_msg)




# EXTRACT FUNTION
def cvmfs_extract(cvmfs_repo, tar_files):   # cvmfs_repo= repo01.infn.it , tar_files=['oidc.tar']
         
         my_cvmfs_repo_extract_path= my_cvmfs_path + "/" + cvmfs_repo + "/to_extract/"
         for tar_file in tar_files:
                extract_time=datetime.now()
                print(f"Extracting {tar_file} in {cvmfs_repo} started at {extract_time}.")
                logging.info(f"Extracting {tar_file} in {cvmfs_repo} started.")
    
                try:
                    # If the repo is in a transaction, it must be closed before ingestion . 
                    resp=subprocess.run(["sudo", "cvmfs_server", "list"], check=True, capture_output=True)
                    resp1 = subprocess.run(["grep", cvmfs_repo], input=resp.stdout, capture_output=True)
                    if "transaction" in resp1.stdout.decode('utf-8'):
                       res=subprocess.run(["cvmfs_server", "publish", cvmfs_repo], capture_output=True, text=True, check=True)
                       logging.info(f"{res.stdout}")
                       if res.stderr:
                          logging.error(f"{res.stderr}")
                    
                    # CVMFS ingest
                    my_tar_file= my_cvmfs_repo_extract_path + tar_file          # my_tar_file= cvmfs/repo21.infn.it/to_extract/oidc-agent.5.1.0.tar
                    subprocess.run(["cvmfs_server", "ingest", "-t", my_tar_file, "-b", "software/" , cvmfs_repo], capture_output=True, text=True, check=True)
                    
                    # Delete .tar files
                    os.remove(os.path.join(my_cvmfs_repo_extract_path, tar_file))
                    
                    extract_finish_time=datetime.now()
                    print(f"CVMFS server ingest process for {tar_file} in {cvmfs_repo} successfully completed at {extract_finish_time}.")
                    logging.info(f"CVMFS server ingest process for {tar_file} in {cvmfs_repo} successfully completed.")

                except subprocess.CalledProcessError as e:
                    error_msg=f"CVMFS server ingest ERROR for {tar_file} in {cvmfs_repo}: {e}. Aborting transaction..."
                    logging.error(error_msg)
                    send_to_zabbix(error_msg)
                    # Transaction abort in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True, text=True, check=False)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       error_msg=f"CVMFS abort error in cvmfs_extract function: {res.stderr}"
                       logging.error(error_msg)
                       send_to_zabbix(error_msg)

                except Exception as e:
                    error_msg=f"Unexpected error in cvmfs_extract function: {e}"
                    logging.error(error_msg)
                    send_to_zabbix(error_msg)




def main():

    setup_logging()
    while True:
        cvmfs_repo_sync()
        time.sleep(TIME_CHECK)   



if __name__ == "__main__":

      try:
        main()

      except KeyboardInterrupt:
        error_msg="Shutdown cvmfs-repo-sync.py script via KeyboardInterrupt."  
        logging.error(error_msg)
        send_to_zabbix(error_msg)
        sys.exit(0)

      except Exception as e:
        error_msg=f"Fatal error in main loop: {e}"
        logging.error(error_msg)
        send_to_zabbix(error_msg)
        sys.exit(1)
