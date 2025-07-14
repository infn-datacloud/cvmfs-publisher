# Docker notes

- cvmfs-repo-consumers, cvmfs-repo-sync and publisher-consumer are independent dockers running the corresponding python scripts. 

-  All the 3 dockers export their logs in /var/log/publisher.

-  cvmfs-repo-consumers and cvmfs-repo-sync dockers share an external disk, /data/cvmfs.


-  On publisher-consumer and cvmfs-repo-sync dockers a CVMFS server environment is running. For this reason, they must use an ext4 volume for the /var/spool/cvmfs partition. This volume can be any ext4 volume; for example, it could be /tmp. This is because OverlayFS on OverlayFS is not supported in Linux. OverlayFS is a union filesystem used by the CVMFS server installed as a Docker. When a transaction is done, CVMFS server attempts to mount another OverlayFS, which results in the following error: overlayfs: filesystem on '/var/spool/cvmfs/xxxx/scratch/current' not supported as upperdir. The solution is to mount a host-compatible volume like ext4.

-  Zabbix agent monitoring is implemented inside the 3 dockers.


## Build docker images
```bash
$ sudo docker build -f cvmfs-repo-consumers/Dockerfile -t cvmfs-repo-consumers .

$ sudo docker build -f sync-publisher/Dockerfile -t sync-publisher .
```


## Instructions docker compose 
```bash
$ sudo COMPOSE_BAKE=true docker-compose up -d
```

