#!/bin/bash

docker run --network=host --rm -v $(pwd):$(pwd) -it confluentinc/cp-kafka "$@"
