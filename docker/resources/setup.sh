#! /bin/bash

cd /home/dss && \
rm -rf /home/dss/home/* && \
su -c "/home/dss/dataiku-dss-5.1.5/installer.sh -d /home/dss/home -p 11000" dss