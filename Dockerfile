FROM ubuntu:precise

WORKDIR /root

ENV repo https://github.com/herry13/cluster-scheduler-simulator

RUN apt-get update && \
    apt-get -y install apt-transport-https

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 && \
    apt-get update && \
    apt-get install -y openjdk-7-jre openjdk-7-jdk sbt git && \
    git clone $repo

CMD ["/bin/bash", "-c", "cd /root/cluster-scheduler-simulator && sbt run"]
