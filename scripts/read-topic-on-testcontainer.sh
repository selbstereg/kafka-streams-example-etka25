#!/bin/bash
TOPIC=$1
PORT=$(docker ps | grep kafka-testcontainer | cut -d':' -f3 | cut -d'-' -f1)
echo "Reading topic \"$TOPIC\" (port $PORT)."

./kafka-tools.sh /bin/kafka-console-consumer \
    --bootstrap-server localhost:$PORT \
    --topic $TOPIC \
    --property print.key=true \
    --timeout-ms 2000 \
    --from-beginning
