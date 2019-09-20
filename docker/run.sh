#! /bin/bash

export DSS_HOME=~/dss_home

docker run -v ${DSS_HOME}:/home/dss/home \
  -p 11000:11000 \
  --rm \
  --net="host" \
  --name dataiku_dss \
  dataiku_dss