FROM ubuntu:18.04

RUN apt-get update && \
    apt-get -y install wget vim

RUN useradd -m dss

RUN cd /home/dss && \
    su -c "wget https://cdn.downloads.dataiku.com/public/dss/5.1.5/dataiku-dss-5.1.5.tar.gz" dss && \
    su -c "tar -zxvf dataiku-dss-5.1.5.tar.gz" dss

RUN apt-get install -y acl curl git nginx unzip zip default-jre-headless python2.7 libpython2.7 libfreetype6 libgfortran3 libgomp1 supervisor

COPY resources/setup.sh /
RUN chmod u+x /setup.sh
COPY resources/start.sh /
RUN chmod u+x /start.sh && \
    chown dss:dss /start.sh
COPY resources/start.conf  /etc/supervisor/conf.d/

#RUN cd /home/dss && \
#    su -c "dataiku-dss-5.1.5/installer.sh -d /home/dss/dss_home -p 11000" dss

CMD ["service", "supervisor", "start"]