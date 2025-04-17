

# CVMFS publisher - RabbitMQ - Ceph RGW interaction

![ScreenShot](images/Publisher-vault-interaction.png)


CVMFS publisher gets notified by RabbitMQ when the content of the cvmfs/ area of the S3 buckets changes, and starts the synchronization with the CVMFS repository.

It is implemented using the cvmfs_repo_consumers.py script. It establishes a secure connection with RabbitMQ to consume messages stored in the users queues.


# Documentations

[User guide](https://confluence.infn.it/display/INFNCLOUD/Software+Management+user+guide)

[CVMFS Service Card](https://confluence.infn.it/display/INFNCLOUD/CVMFS+Service+Card)