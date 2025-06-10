# docker-compose instructions

COMPOSE_BAKE=true docker-compose up --build -d


# CVMFS network cvmfs-publisher-docker-network

docker network create cvmfs-publisher-docker-network

The cvmfs-publisher-docker-network network has MTU=1450