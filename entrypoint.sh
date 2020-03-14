#! /bin/bash
# Submit application to spark cluster on client mode.

APPLICATION_JAR_FILE=$1
shift

$SPARK_HOME/bin/spark-submit \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --deploy-mode client \
  $SPARK_SUBMIT_ARGS \
  $APPLICATION_JAR_FILE \
  --master spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT \
  --file $JAVA_PROJECT_DATASET \
  $@
