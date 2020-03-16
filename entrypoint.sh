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

if [ -z "${SPARK_DRIVER_HOST}" ]; then
  export SPARK_DRIVER_HOST=$(hostname -I)
fi

if [ -z "${SPARK_DEPLOY_MODE}" ]; then
  export SPARK_DEPLOY_MODE=client
fi

$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -mkdir -p /spark
$HADOOP_HOME/bin/hdfs dfs -fs hdfs://$HADOOP_HOST:$HADOOP_PORT -put -f $JAVA_PROJECT_DATASET /spark/dataset.csv

echo '>>' connecting to Master @ spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
echo '>>' file submited @ hdfs://$HADOOP_HOST:$HADOOP_PORT
echo '>>' deploy mode: $SPARK_DEPLOY_MODE
echo '>>' java jar file loaded from $APPLICATION_JAR_FILE

$SPARK_HOME/bin/spark-submit \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --deploy-mode $SPARK_DEPLOY_MODE \
  --conf spark.driver.host=$SPARK_DRIVER_HOST \
  $SPARK_SUBMIT_ARGS \
  $APPLICATION_JAR_FILE \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --file hdfs://$HADOOP_HOST:$HADOOP_PORT/spark/dataset.csv \
  $@
