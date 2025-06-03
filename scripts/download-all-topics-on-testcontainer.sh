#!/bin/bash

rm ../etka25-app*

TOPICS=$(./list-topics-on-testcontainer.sh | tr -d '\r')

for topic in $TOPICS; do
        ./read-topic-on-testcontainer.sh $topic > ../$topic.txt
done
