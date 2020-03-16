FROM middleware/spark-base:2.4.4-hadoop2.7

RUN apt-get update \
     && apt-get install -y wget ssh rsync supervisor \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

# SSH
RUN ssh-keygen -t rsa -P "" -f $HOME/.ssh/id_rsa \
     && cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

# HADOOP Configuration
COPY conf/*.xml ./
RUN mv ./*.xml $HADOOP_HOME/etc/hadoop/

EXPOSE 9000 8020 50070 8032 8088

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]
