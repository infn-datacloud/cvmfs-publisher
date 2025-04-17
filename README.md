

# CVMFS publisher - RabbitMQ - Vault interaction

![ScreenShot](images/Publisher-vault-interaction.png)

CVMFS publisher is notified when new CVMFS repositories are created to retrieve the repository keys from Vault and make the repository accessible to the publisher via the gateway.

It is implemented using the [publisher_consumer.py](https://baltig.infn.it/infn-cloud/wp6/cvmfs-publisher/-/blob/main/scripts/publisher_consumer.py?ref_type=heads) script. 

It establishes a secure connection with RabbitMQ to digest messages stored in the publisher queue.
In order to interact with RabbitMQ, the publisher RabbitMQ user with limited privileges (tag = impersonator) is used, with certificates required for the ssl connection.
        
Messages contain information about the CVMFS repositories to be created:

<AAI name>,<iam subject>,<repo-name>,<type>

where type=P for personal repo, type=G for group repo.

With this information, the application authenticates to Vault via a read_only AppRole, downloads the keys, creates the CVMFS repositories, connect to RGW and create the topic, connect to RabbitMQ and create the corresponding queue. 



# CVMFS publisher - RabbitMQ - Ceph RGW interaction

![ScreenShot](images/Cephrwg-rabbitmq-publisher.png)


The user populates his repository by accessing his own S3 bucket via the web application https://s3webui.cloud.infn.it/ and upload the software he wants to distribute in the cvmfs bucket. 

User can also populate the repo using the standard CVMFS mechanisms by a CVMFS publisher (for advanced users).

As soon as the user uploads software to the bucket, the system is notified and start synchronizing the contents of the bucket with the corresponding CVMFS repository.

CVMFS publisher gets notified by RabbitMQ when the content of the cvmfs/ area of the S3 buckets changes, and starts the synchronization with the CVMFS repository.

Using the cvmfs_repo_consumers.py script, it establishes a secure connection with RabbitMQ to consume messages stored in the users queues.

The cvmfs_repo_sync.py script syncronizes the content of the /data/cvmfs/<reponame> folders with the corresponding CVMFS repositories.


# Documentations

[User guide](https://confluence.infn.it/display/INFNCLOUD/Software+Management+user+guide)

[CVMFS Service Card](https://confluence.infn.it/display/INFNCLOUD/CVMFS+Service+Card)