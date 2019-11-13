#!/bin/sh
echo "-----------ELK------------"
docker-compose -f ./docker-elk/docker-compose.yml down -v
echo "----------Kafka-----------"
docker-compose -f kafka.yml down
echo "--Microservice Collector--"
docker-compose -f ./ms_collector/ms_collector.yml down
echo "--------Producers---------"
docker-compose -f ./producer/producers.yml down
#docker network rm tfm
docker ps -a