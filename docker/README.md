# Docker 
## docker-compose.yaml
### cvmfs-publisher-docker-network

The docker-compose.yaml uses a special docker network with MTU = 1450 due to a INFN Cloud network constain.

1. Save the docker binary:
```bash
$ sudo mv /usr/bin/docker /usr/bin/docker.bin
``` 

2. Create a docker wrapper script to set MTU to 1450, used only when the 'docker network create' command is executed:
```bash
 $ sudo vi /usr/bin/docker
```

```bash
#!/usr/bin/env bash
    MTU=1450
    DOCKER_BIN="/usr/bin/docker.bin" 
    if [[ "$1" == "network" && "$2" == "create" ]]; then
        shift 2
        "$DOCKER_BIN" network create --opt com.docker.network.driver.mtu="$MTU" "$@"
    else
        "$DOCKER_BIN" "$@"
    fi
```

3. Give execute permissions on the new wrapper:
```bash
$ sudo chmod +x /usr/bin/docker
```

4. Create the new network:ì
```bash
$ sudo docker network create cvmfs-publisher-docker-network
```

5. Verify the new network:
```bash
$ sudo docker network inspect cvmfs-publisher-docker-network
```

6. Create the docker using this network:
sudo docker run -d --name cvmfs-repo-consumers --network cvmfs-publisher-docker-network -v /var/log/publisher:/var/log/publisher -v /data/cvmfs:/data/cvmfs cvmfs-repo-consumers


## docker-compose instructions

$ sudo COMPOSE_BAKE=true docker-compose up --build -d


