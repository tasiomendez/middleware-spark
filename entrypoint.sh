#! /bin/bash
# Submit application to spark cluster on client mode.

APPLICATION_JAR_FILE=$1
shift

# Configuration of hadoop host
if [ -z "${HADOOP_HOST}" ]; then
  export HADOOP_HOST=hadoop
fi

if [ -z "${HADOOP_PORT}" ]; then
  export HADOOP_PORT=9000
fi

$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -mkdir /spark
$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -put $JAVA_PROJECT_DATASET /spark/dataset.csv

$SPARK_HOME/bin/spark-submit \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --deploy-mode client \
  $SPARK_SUBMIT_ARGS \
  $APPLICATION_JAR_FILE \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --file hdfs://$HADOOP_HOST:$HADOOP_PORT/spark/dataset.csv \
  $@
