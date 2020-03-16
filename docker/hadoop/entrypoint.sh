#! /bin/bash

# Export JAVA_HOME to .bashrc
echo "export JAVA_HOME=$JAVA_HOME" >> $HOME/.bashrc

# Configuration of hadoop host
if [ -z "${HADOOP_HOST}" ]; then
  export HADOOP_HOST=$(hostname -I)
fi

if [ -z "${HADOOP_PORT}" ]; then
  export HADOOP_PORT=9000
fi

sed -i "s|{{HADOOP_HOST}}|$HADOOP_HOST|g" $HADOOP_HOME/etc/hadoop/core-site.xml
sed -i "s|{{HADOOP_PORT}}|$HADOOP_PORT|g" $HADOOP_HOME/etc/hadoop/core-site.xml

# Start ssh service
/etc/init.d/ssh start

ssh-keyscan -H 127.0.0.1 >> $HOME/.ssh/known_hosts
ssh-keyscan -H 0.0.0.0   >> $HOME/.ssh/known_hosts
ssh-keyscan -H localhost >> $HOME/.ssh/known_hosts
ssh-keyscan -H $HADOOP_HOST >> $HOME/.ssh/known_hosts

# Start hadoop
$HADOOP_HOME/bin/hdfs namenode -format 1> /dev/null
$HADOOP_HOME/sbin/start-dfs.sh

tail -F $HADOOP_HOME/logs/*-datanode-*.log
