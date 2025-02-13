# my_cvmfs_repo_sync.py
# Author: Francesca Del Corso
# Last update: Feb 11 2025
# Syncronization between /data/cvmfs folders on the cvmfs_publish host and the corresponding CVMFS repositories.

import os,sys
import re
import shutil
import subprocess
import time
import logging
from datetime import datetime

my_cvmfs_path = r"/data/cvmfs"                         # Folder path to monitor
cvmfs_path    = r"/cvmfs/"                             # CVMFS repo base pat

# This function deletes multipart uplod files in the CVMFS repositories. Their filename ends with a period followed by 8 characters
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
                print(f"Deleting temparary file {filename} ...")
                os.remove(file_path)
                print(f"{filename} successfully deleted")
            except Exception as e:
                print(f"Error deleting {filename}: {e}")


# This function syncronizes the publisher folders with the cvmfs repositories.
def cvmfs_repo_sync(my_cvmfs_path, cvmfs_path):         
    # my_cvmfs_path=/data/cvmfs , cvmfs_path=/cvmfs , cvmfs_repo=repo01.infn.it 
    for cvmfs_repo in os.listdir(my_cvmfs_path): 
        
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
                    # Start a CVMFS transaction if the repo is not yet in a transaction.
                    # 'sudo cvmfs_server list | grep repo' must contain the string "in transaction"
                    resp=subprocess.run(["sudo", "cvmfs_server", "list"], check=True, capture_output=True)
                    resp1 = subprocess.run(["grep", cvmfs_repo], input=resp.stdout, capture_output=True)
                    if not "transaction" in resp1.stdout.decode('utf-8'):
                       res=subprocess.run(["cvmfs_server", "transaction", cvmfs_repo], capture_output=True,text=True, check=True)

                       logging.info(f"{res.stdout}")
                       if res.stderr:
                          logging.error(f"{res.stderr}")

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
                    
                    # Execute CVMFS publish
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
                    print(f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction.")
                    logging.info(f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction.")
                    # Transaction abort in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True,text=True,check=False)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       logging.error(f"{res.stderr}")

                except Exception as e:
                    print(f"Unexpected error: {e}")
                    logging.error(f"Unexpected error: {e}")
           
            # case files to DELETE
            files_to_delete = my_cvmfs_path + "/" + cvmfs_repo + "/to_delete/"  + cvmfs_repo.split('.')[0] + "-infn-it.txt"
            if os.path.exists(files_to_delete) and (os.path.getsize(files_to_delete) > 0):  # check file existance and not empty
                print(f"Starting removing files for {cvmfs_repo} CVMFS repository ..")
                logging.info(f"Starting removing files for {cvmfs_repo} CVMFS repository ..")
                delete_cvmfs_files(files_to_delete,cvmfs_repo)

            # case .tar files to EXTRACT
            to_extract_path = my_cvmfs_path + "/" + cvmfs_repo + "/to_extract/"
            if os.path.isdir(to_extract_path) and os.listdir(to_extract_path):   
                   tar_files = [f for f in os.listdir(to_extract_path) if os.path.isfile(os.path.join(to_extract_path, f))]
                   cvmfs_extract(cvmfs_repo,tar_files)  


def delete_cvmfs_files(to_delete_file,cvmfs_repo):
         
         with open(to_delete_file, "r") as f:
              files = f.readlines()    
         
         remaining_files = []       
         # Open a CVMFS transaction
         try:
            # Start a CVMFS transaction if the repo is not yet in a transaction.
            resp=subprocess.run(["sudo", "cvmfs_server", "list"], check=True, capture_output=True)
            resp1 = subprocess.run(["grep", cvmfs_repo], input=resp.stdout, capture_output=True)
            if not "transaction" in resp1.stdout.decode('utf-8'):
                    res=subprocess.run(["cvmfs_server", "transaction", cvmfs_repo], capture_output=True,text=True, check=True)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                          logging.error(f"{res.stderr}")

            # Delete files
            for file in files:
             file_path = file.strip()        # file_path=/cvmfs/repo01.infn.it/oidc-agent_5.1.0.tar
             if file_path.endswith('.tar'):  # CASE .tar file delete
                try:
                   # The entire folder is to be deleted
                   folder_name = os.path.splitext(os.path.basename(file_path))[0]  
                   file_path_dir = "/cvmfs/" + cvmfs_repo + "/software/" + folder_name
                   shutil.rmtree(file_path_dir)
                   print(f"{file_path_dir} deleted successfully.")
                
                except Exception as e:
                   print(f"Failed to delete {file_path}: {e}")
                   logging.info(f"Failed to delete {file_path}: {e}")
                   remaining_files.append(file)  # Keep the file if deletion fails

             else:
               try:
                 if os.path.exists(file_path):
                   os.remove(file_path)
                   print(f"Deleted: {file_path}")
                   logging.info(f"Deleted: {file_path}")
                 else:
                   # case file not found is not an error, it is only reported in the log file.
                   print(f"File not found, skipping: {file_path}")
                   logging.info(f"File not found, skipping: {file_path}")

               except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
                logging.info(f"Failed to delete {file_path}: {e}")
                remaining_files.append(file)  # Keep the file if deletion fails
        

            # Execute CVMFS publish
            res=subprocess.run(["cvmfs_server", "publish", cvmfs_repo], capture_output=True,text=True,check=True)
            logging.info(f"{res.stdout}")
            if res.stderr:
                        print(f"CVMFS publish for {cvmfs_repo} error: {res.stderr}")
                        logging.error(f"CVMFS publish for {cvmfs_repo} error: {res.stderr}")
            else:
                        print(f"CVMFS publish for {cvmfs_repo} successfully completed.")
                        logging.info(f"CVMFS publish for {cvmfs_repo} successfully completed.")


            # Rewrite the file with remaining paths
            with open(to_delete_file, "w") as f:
                 f.writelines(remaining_lines) 

            del_end_time=datetime.now()
            print(f"DELETE file operation for {cvmfs_repo} successfully completed at {del_end_time}.")
            logging.info(f"DELETE file operation for {cvmfs_repo} successfully completed.")

         except subprocess.CalledProcessError as e:
                    print(f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction.")
                    logging.info(f"CVMFS transaction ERROR for {cvmfs_repo} repository: {e}, aborting transaction.")
                    # Transaction abort in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True,text=True,check=False)
                    print(f"{res.stdout}")
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       logging.error(f"{res.stderr}")

         except Exception as e:
                    print(f"Unexpected error: {e}")
                    logging.error(f"Unexpected error: {e}")




# EXTRACT FUNTION
def cvmfs_extract(cvmfs_repo, tar_files):   # cvmfs_repo= repo01.infn.it , tar_files=['oidc.tar']
         
         my_cvmfs_repo_extract_path= my_cvmfs_path + "/" + cvmfs_repo + "/to_extract/"
         for tar_file in tar_files:
                extract_time=datetime.now()
                print(f"Extracting {tar_file} in {cvmfs_repo} started at {extract_time}.")
                logging.info(f"Extracting {tar_file} in {cvmfs_repo} started.")
    
                try:
                    # 'sudo cvmfs_server list | grep repo' must contain the string "in transaction"
                    resp=subprocess.run(["sudo", "cvmfs_server", "list"], check=True, capture_output=True)
                    resp1 = subprocess.run(["grep", cvmfs_repo], input=resp.stdout, capture_output=True)
                    # If the repo is in a transaction, close the transaction with publish before doing the ingest operation. 
                    if "transaction" in resp1.stdout.decode('utf-8'):
                       res=subprocess.run(["cvmfs_server", "publish", cvmfs_repo], capture_output=True, text=True, check=True)
                       logging.info(f"{res.stdout}")
                       if res.stderr:
                          logging.error(f"{res.stderr}")
                    
                    # CVMFS ingest
                    my_tar_file= my_cvmfs_repo_extract_path + tar_file
                    # my_tar_file= cvmfs/repo21.infn.it/to_extract/oidc-agent.5.1.0.tar
                    subprocess.run(["cvmfs_server", "ingest", "-t", my_tar_file, "-b", "software/" , cvmfs_repo], capture_output=True, text=True, check=True)
                    # Delete .tar files
                    os.remove(os.path.join(my_cvmfs_repo_extract_path, tar_file))
                    extract_finish_time=datetime.now()
                    print(f"CVMFS server ingest process for {tar_file} in {cvmfs_repo} successfully completed at {extract_finish_time}.")
                    logging.info(f"CVMFS server ingest process for {tar_file} in {cvmfs_repo} successfully completed.")

                except subprocess.CalledProcessError as e:
                    print(f"CVMFS server ingest ERROR for {tar_file} in {cvmfs_repo}: {e}. Aborting transaction...")
                    logging.info(f"CVMFS server ingest ERROR for {tar_file} in {cvmfs_repo}: {e}. Aborting transaction ...")
                    # Transaction abort in case of error
                    res=subprocess.run(["cvmfs_server", "abort", "-f", cvmfs_repo], capture_output=True, text=True, check=False)
                    logging.info(f"{res.stdout}")
                    if res.stderr:
                       logging.error(f"{res.stderr}")

                except Exception as e:
                    print(f"Unexpected error: {e}")
                    logging.error(f"Unexpected error: {e}")


def log_generation():
    # Generate log file with current date
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"/var/log/publisher/cvmfs_repo_sync-{date_stamp}.log"
    logging.basicConfig(
     level=logging.INFO,                    # Set the logging level: INFO, ERROR, DEBUG
     filename=log_filename,                 # Specify log file name
     filemode='a',                          # Append to the file if it exists
     format='%(asctime)s - %(levelname)s - %(message)s'
    )


def main():

    log_generation()
    while True:
        cvmfs_repo_sync(my_cvmfs_path, cvmfs_path)
        time.sleep(60)                                  # Wait 60 seconds before check again 



if __name__ == "__main__":

      try:
        main()

      except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

