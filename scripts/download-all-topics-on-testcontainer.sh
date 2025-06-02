#!/bin/bash

rm ~/tmp/etka25-app*

TOPICS=$(./list-topics-on-testcontainer.sh | tr -d '\r')

for topic in $TOPICS; do
        ./read-topic-on-testcontainer.sh $topic > ~/tmp/$topic.txt
done
