#export VERSION=0.10.2.1
export VERSION=1.1.0
docker-compose up -d

alias topics="docker run --network services_default -it wurstmeister/kafka:$VERSION kafka-topics.sh --zookeeper zookeeper:2181"

IS_CREATED=`topics --describe --topic test | wc -l`

if [[ IS_CREATED == 0 ]]; then
    echo "Creating test topic"
    topics --create --topic test --partitions 1 --replication-factor 1
else
    echo "test topic exists"
fi