export VERSION=0.10.2.1
#export VERSION=1.1.0
docker-compose up -d

TOPIC=test

alias topics="docker run --network services_default -it wurstmeister/kafka:$VERSION kafka-topics.sh --zookeeper zookeeper:2181"

IS_CREATED=`topics --describe --topic $TOPIC | wc -l`

if [[ $IS_CREATED -eq 0 ]]; then
    echo "Creating $TOPIC topic"
    topics --create --topic $TOPIC --partitions 1 --replication-factor 1
else
    echo "$TOPIC topic exists"
fi