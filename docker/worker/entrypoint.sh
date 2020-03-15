#! /bin/bash
# Starts a slave on the machine this script is executed on.

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "$SPARK_HOME/sbin/spark-config.sh"

. "$SPARK_HOME/bin/load-spark-env.sh"

if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi

if [ "$SPARK_MASTER_HOST" = "" ]; then
  SPARK_MASTER_HOST=`hostname`
fi

if [ "$SPARK_WOKER_HOST" = "" ]; then
  SPARK_WOKER_HOST=`hostname`
fi

if [ "$SPARK_WORKER_WEBUI_PORT" = "" ]; then
  SPARK_WORKER_WEBUI_PORT=8081
fi

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --host $SPARK_WOKER_HOST \
  --webui-port $SPARK_WORKER_WEBUI_PORT \
  spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
