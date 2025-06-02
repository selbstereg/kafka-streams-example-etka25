#!/bin/bash
TOPIC=$1
PORT=$(docker ps | grep kafka-testcontainer | cut -d':' -f3 | cut -d'-' -f1)

./kafka-tools.sh /bin/kafka-topics --bootstrap-server localhost:$PORT --list
