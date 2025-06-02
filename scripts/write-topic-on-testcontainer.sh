#!/bin/bash
TOPIC=$1
PORT=$(docker ps | grep kafka-testcontainer | cut -d':' -f3 | cut -d'-' -f1)
echo "Writing to topic \"$TOPIC\" (port $PORT)."

./kafka-tools.sh /bin/kafka-console-producer \
  --bootstrap-server localhost:$PORT \
  --topic $TOPIC \
  --property "parse.key=true" \
  --property "key.separator=|"
