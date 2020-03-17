#! /bin/bash
# Submit application to spark cluster on client mode.

JAVA_JAR_FILENAME=$1
shift

if [ -z "${SPARK_DRIVER_HOST}" ]; then
  export SPARK_DRIVER_HOST=$(hostname -I)
fi

echo '>>' connecting to Master @ spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
echo '>>' files submited @ hdfs://$HADOOP_HOST:$HADOOP_PORT
echo '>>' jar: $JAVA_JAR_FILENAME
echo '>>' class: $JAVA_JAR_MAIN_CLASS

$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -mkdir -p /spark
echo '>>' submiting jar file...
$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -put -f $JAVA_JAR_FILENAME /spark/application.jar
echo '>>' submiting dataset file...
$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -put -f $JAVA_PROJECT_DATASET /spark/dataset.csv

$SPARK_HOME/bin/spark-submit \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --class $JAVA_JAR_MAIN_CLASS \
  --conf spark.driver.host=$SPARK_DRIVER_HOST \
  $SPARK_SUBMIT_ARGS \
  hdfs://$HADOOP_HOST:$HADOOP_PORT/spark/application.jar \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --file hdfs://$HADOOP_HOST:$HADOOP_PORT/spark/dataset.csv \
  $@
