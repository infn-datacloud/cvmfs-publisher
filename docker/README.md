# docker-compose.yaml

The docker-compose.yaml uses a special docker network with MTU = 1450 due to a INFN Cloud network constain.

Steps:
1. Save the docker binary:

    $ sudo mv /usr/bin/docker /usr/bin/docker.bin

2. Create a docker wrapper script to set MTU to 1450, used only when the 'docker network create' command is executed:

```bash
#!/usr/bin/env bash
    MTU=1450
    DOCKER_BIN="/usr/bin/docker.bin" 
    if [[ "$1" == "network" && "$2" == "create" ]]; then
        shift 2
        "$DOCKER_BIN" network create --opt com.docker.network.driver.mtu="$MTU" "$@"
    else
        # Forward all other commands as-is
        "$DOCKER_BIN" "$@"
    fi
```


    $ sudo vi /usr/bin/docker

    

3. Give execute permissions on the new wrapper:
$ sudo chmod +x /usr/bin/docker

4. Create the new network:
$ sudo docker network create cvmfs-publisher-docker-network

5. Verify the new network:
$ sudo docker network inspect cvmfs-publisher-docker-network

6. Create the docker using this network:
sudo docker run -d --name cvmfs-repo-consumers --network cvmfs-publisher-docker-network -v /var/log/publisher:/var/log/publisher -v /data/cvmfs:/data/cvmfs cvmfs-repo-consumers


# docker-compose instructions

$ sudo COMPOSE_BAKE=true docker-compose up --build -d

*************************************************************************************************************************

## RUCIO deployment
### Prepare secrets

```bash
kubectl create ns rucio
kubectl create secret -n rucio generic rucio-daemons-fts-cert \
--from-file=usercert.pem=./certs/usercert.pem

kubectl create secret -n rucio generic rucio-daemons-fts-key \
--from-file=new_userkey.pem=./certs/userkeyNoEncr.pem
```

### Prepare OIDC sync components

Please follow instructions [here](./scripts/create-iam-clients/README.md). UNDER DEVELOPMENT

### User login test

Install the rucio client via `pip install -U rucio-clients`

Copy the config file `docker/oidc-sync/rucio/etc/rucio.cfg` into `$RUCIO_HOME/etc/rucio.cfg` where RUCIO_HOME is whatever you set in your environement via:

```bash
export RUCIO_HOME=<YOUR FOLDER HERE>
export RUCIO_ACCOUNT=<YOUR AAI USERNAME>
```

Now you are ready to trigger the authN procedure:

```bash
rucio whoami
```

## Setup flux to manage the cluster

- Flux cli installation: `curl -s https://fluxcd.io/install.sh | sudo bash`
- Install metric server: `kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml`
- Edit the deployment (`kubectl edit deployment -n kube-system metrics-server`) to skip CA verification, adding the following option: `--kubelet-insecure-tls`
- Create a gitlab access token for this repo with access with the following permissions: api, read repo, and write repo
- Flux bootstrap pointing to a dedicated folder in this baltig repo:
```bash
export GITLAB_TOKEN=<generated access token>
flux bootstrap gitlab \
               --hostname=baltig.infn.it \
               --token-auth \
               --owner=infn-cloud/wp6 \
               --repository=k8s-infra \
               --branch=main \
               --path=clusters/<name of the cluster>
```

After the sync, remember to update the externalIP in the ngnix cluster values.