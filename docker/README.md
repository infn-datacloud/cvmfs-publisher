

# CVMFS publisher - RabbitMQ - Vault interaction

![ScreenShot](images/Publisher-vault-interaction.png)

CVMFS publisher is notified when new CVMFS repositories are created to retrieve the repository keys from Vault and make the repository accessible to the publisher via the gateway.
It is implemented using the [publisher_consumer.py](https://baltig.infn.it/infn-cloud/wp6/cvmfs-publisher/-/blob/main/scripts/publisher_consumer.py?ref_type=heads) script. 
It establishes a secure connection with RabbitMQ to digest messages stored in the publisher queue.
The application authenticates to Vault via a read_only AppRole, downloads the keys, creates the CVMFS repositories, connect to RGW and create the topic, connect to RabbitMQ and create the corresponding queue. 


