#!/bin/sh
echo "---Creating tmf network---"
docker network create tfm
echo "-----------ELK------------"
docker-compose -f ./docker-elk/docker-compose.yml up -d
sleep 10
echo "----------Kafka-----------"
docker-compose -f kafka.yml up -d
sleep 10
echo "--Microservice Collector--"
docker-compose -f ./ms_collector/ms_collector.yml up -d
sleep 3
echo "--------Producers---------"
docker-compose -f ./producer/producers.yml up -d
docker ps
sleep 30
open -a /Applications/Google\ Chrome.app http://localhost:5601
open -a /Applications/Google\ Chrome.app http://localhost:9200 