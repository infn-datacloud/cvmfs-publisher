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
# limitations under the License.

import pika
import sys
import os
import json
import hvac
import subprocess
from datetime import datetime
import logging
import random
import string
import shutil
import ssl
from logging.handlers import TimedRotatingFileHandler


with open("./parameters.json") as json_data_file:
    data = json.load(json_data_file)

cvmfs_config = data["cvmfs"]
gw_config = data["cvmfs-gateway"]
rabbitmq_config = data["rabbitMQ"]
vault_config = data["vault"]
ssl_config = data["ssl"]


# SSL context configuration
context = ssl.create_default_context(cafile=ssl_config["ca_cert"])
context.load_cert_chain(certfile=ssl_config["client_cert"], keyfile=ssl_config["client_key"])


# Subprocess cmd
def run_cmd(cmd, capture=False):
    try:
        result = subprocess.run(cmd, capture_output=capture, check=True, text=True)
        return result.stdout if capture else None
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {' '.join(cmd)} - {e}")
        raise


# Vault login
def vault_login_approle(client):
    try:
        client.auth.approle.login(role_id=vault_config["role-id"], secret_id=vault_config["secret-id"])
        logging.info("Login to Vault server successful.")
    except Exception as e:
        logging.info(f"Vault login error: {e}")


# Vault key publication
def vault_key_publication(subject, repository_name, type_repo_group):
    try:
        client = hvac.Client(vault_config["url"])
        vault_login_approle(client)
        logging.info(f"VAULT client authenticated: {client.is_authenticated()}. Init: {client.sys.is_initialized()}, Sealed: {client.sys.is_sealed()}.")

        key_path = cvmfs_config["repo_key_path"]
        keys = {}
        for suffix in [".pub", ".masterkey", ".crt", ".gw"]:
            with open(os.path.join(key_path, repository_name + suffix), "r") as f:
                keys[suffix] = f.read()

        path_ = f"{subject}/cvmfs_keys/{repository_name}" if type_repo_group == "P" else f"groups/{repository_name.split('.')[0]}/cvmfs_keys/{repository_name}"

        client.secrets.kv.v2.create_or_update_secret(
            path=path_,
            secret={
                "publicKey": keys[".pub"],
                "masterKey": keys[".masterkey"],
                "certificateKey": keys[".crt"],
                "gatewayKey": keys[".gw"],
            },
        )
        logging.info("Vault keys publishing done successfully.")
        return "Y"
    except Exception as e:
        logging.error(f"Vault key publication error: {e}")
        return e


# Restart cvmfs-gateway
def cvmfs_gw_service_restart():
    run_cmd(["sudo", "systemctl", "restart", "cvmfs-gateway.service"])
    logging.info("cvmfs-gateway.service restarted successfully.")


# CVMFS gateway secret generation
def cvmfs_gw_sgen(secret_len):
    characters = string.ascii_letters + string.digits + "!$&?#"
    return ''.join(random.choice(characters) for _ in range(secret_len))


# Update /etc/cvmfs/gateway/repo.json
def cvmfs_gw_repo_json_update(repo_json_file, key, value):
    with open(repo_json_file, "r") as f:
        data = json.load(f)
    data.setdefault(key, [])
    if value not in data[key]:
        data[key].append(value)
    with open(repo_json_file, "w") as f:
        json.dump(data, f, indent=2)


def cvmfs_gw_kgen(repository_name):
    random_string = cvmfs_gw_sgen(gw_config["gwKeySecretLenght"])
    gw_file_path = os.path.join(cvmfs_config["repo_key_path"], f"{repository_name}.gw")
    with open(gw_file_path, "w") as f:
        f.write(f"plain_text keygw{repository_name.split('.')[0]} {random_string}")

    cvmfs_gw_repo_json_update(gw_config["cvmfs_gw_repo_json_file"], "repos", repository_name)
    cvmfs_gw_service_restart()
    shutil.copy2(gw_file_path, gw_config["cvmfs_gw_bck_key_path"])


# Resign repo and update cron
def cvmfs_repo_resign(repository_name):
    try:
        run_cmd(["sudo", "/usr/bin/cvmfs_server", "resign", repository_name])
        logging.info(f"CVMFS repo {repository_name} resign successfully.")
        try:
            with open(cvmfs_config["cronFile"], "r") as f:
                lines = f.readlines()
            last_min = int(lines[-1].split()[0]) + 2 if lines else 0
            cron_entry = f"{last_min % 60} {1 if last_min >= 60 else 0} * * * /usr/bin/cvmfs_server resign {repository_name}\n"
        except Exception as e:
            cron_entry = f"0 1 * * * /usr/bin/cvmfs_server resign {repository_name}\n"
            logging.info(f"Error reading cron file: {e}")
        with open(cvmfs_config["cronFile"], "a") as f:
            f.write(cron_entry)
            logging.info(f"Crontab updated for {repository_name}.")
    except Exception as e:
        logging.error(f"Repo resign error: {e}")


# Add Garbage Collector configuration
def cvmfs_add_gc_conf(reponame):
    conf_file = f"/etc/cvmfs/repositories.d/{reponame}/server.conf"
    if not os.path.exists(conf_file):
        logging.info(f"Error: {conf_file} does not exist.")
        return
    gc_lines = [
        "# Enable garbage collector",
        'CVMFS_AUTO_GC_TIMESPAN="30 days ago"',
        f"CVMFS_GC_DELETION_LOG=/var/log/cvmfs/{reponame}-gc.log ",
        "",
    ]
    with open(conf_file, "a") as f:
        f.write("\n".join(gc_lines))
        logging.info(f"GC config added to {conf_file}.")


# Backup and symlink
def backup_and_symlink(path, target):
    if os.path.exists(path):
        os.rename(path, f"{path}.original")
        logging.info(f"Renamed {path} to {path}.original")
    else:
        logging.info(f"{path} does not exist. Skipping.")
    os.symlink(target, path)
    logging.info(f"Symlink created: {path} -> {target}")


# Update keys
def update_keys(reponame):
    keys_dir = "/etc/cvmfs/keys/"
    backup_and_symlink(os.path.join(keys_dir, f"{reponame}.masterkey"), "common.infn.it.masterkey")
    backup_and_symlink(os.path.join(keys_dir, f"{reponame}.pub"), "common.infn.it.pub")


# CVMFS repository creation
def cvmfs_repo_creation(message):
    try:
        AAIusername, subject, repository_name, issuer_url, commonKey = message.split(",")
        logging.info("Starting CVMFS repository creation .....")
        run_cmd([
            "cvmfs_server", "mkfs", "-z", "-G", "30 days ago", "-s", cvmfs_config["S3_cfg"],
            "-w", cvmfs_config["S3_url"], "-o", "root", repository_name
        ])
        cvmfs_add_gc_conf(repository_name)
        if commonKey == "Y":
            update_keys(repository_name)
        cvmfs_repo_resign(repository_name)
        cvmfs_gw_kgen(repository_name)

        type_repo_group = "P" if (AAIusername + ".infn.it") == repository_name else "G"
        result = vault_key_publication(subject, repository_name, type_repo_group)
        if result == "Y":
            cvmfs_reply_send(AAIusername, subject, repository_name, result, "-")
            cvmfs_repo_log_creation(AAIusername, subject, repository_name, issuer_url)
            publisher_send(AAIusername, subject, repository_name, type_repo_group)
        else:
            logging.info("Vault problem — repo created, keys NOT published.")
            cvmfs_reply_send(AAIusername, subject, repository_name, "N", "Vault keys not published.")
    except Exception as e:
        logging.error(f"Repo creation error: {e}")
        cvmfs_reply_send(AAIusername, subject, repository_name, "N", str(e))


# Log creation
def cvmfs_repo_log_creation(AAIusername, subject, repository_name, issuer_url):
    with open(cvmfs_config["repo_creation_log"], "a") as f:
        f.write(f"{datetime.now()}\t{AAIusername}\t{subject}\t{repository_name}\t{issuer_url}\n")


# Send to RabbitMQ queue
def send_to_queue(queue_name, body):
    credentials = pika.PlainCredentials(rabbitmq_config["username"], rabbitmq_config["password"])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_config["host"],
            port=rabbitmq_config["port"],
            credentials=credentials,
            ssl_options=pika.SSLOptions(context, server_hostname=rabbitmq_config["hostname"]),
        )
    )
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True, arguments={"x-queue-type": "quorum"})
        channel.basic_publish(exchange="", routing_key=queue_name, body=body)
        logging.info(f"Sent to {queue_name} queue: {body}")
    finally:
        connection.close()


def cvmfs_reply_send(AAIusername, subject, repository_name, result, error):
    body = f"{AAIusername},{subject},{repository_name},{result},{error}"
    send_to_queue(rabbitmq_config["cvmfs_reply_queue"], body)


def publisher_send(AAIusername, subject, repository_name, type_repo_group):
    body = f"{AAIusername},{subject},{repository_name},{type_repo_group}"
    send_to_queue(rabbitmq_config["publisher_queue"], body)


# Callback
def callback(ch, method, properties, body):
    message = body.decode("utf-8")
    logging.info(f" [*] {message}")
    cvmfs_repo_creation(message)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Log initialization
def cvmfs_logfile_initialization():
    with open(cvmfs_config["repo_creation_log"], "a") as f:
        f.write("Timestamp\t\t\tAAI_name\tSubject\t\t\t\tRepository\t\tIAM_issuer_url\n")


# Setup logging
def setup_logging():
    date_stamp = datetime.now().strftime("%Y-%m-%d")
    log_file = f"/var/log/cvmfs_repo_agent/cvmfs_repo_agent-{date_stamp}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[TimedRotatingFileHandler(log_file, when='D', interval=7)]
    )



def main():
    setup_logging()
    cvmfs_logfile_initialization()
    credentials = pika.PlainCredentials(rabbitmq_config["username"], rabbitmq_config["password"])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_config["host"],
            port=rabbitmq_config["port"],
            credentials=credentials,
            ssl_options=pika.SSLOptions(context, server_hostname=rabbitmq_config["hostname"]),
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=rabbitmq_config["cvmfs_queue"], durable=True, arguments={"x-queue-type": "quorum"})
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=rabbitmq_config["cvmfs_queue"], on_message_callback=callback, auto_ack=False)
    logging.info(" [*] Waiting for messages. To exit press CTRL+C.")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Keyboard Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

