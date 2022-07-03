#!/bin/bash

docker pull elasticdump/elasticsearch-dump:v6.28.0

docker run --rm -ti -v $(pwd):/dump --network infoint-project-2022_default elasticdump/elasticsearch-dump:v6.28.0 \
  --input=/dump/corporates.dump \
  --output=http://elasticsearch:9200/corporates-imported \
  --type=data