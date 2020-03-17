#! /bin/bash
# Starts the master on the machine this script is executed on.

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "$SPARK_HOME/sbin/spark-config.sh"

. "$SPARK_HOME/bin/load-spark-env.sh"

if [ "$SPARK_MASTER_HOST" = "" ]; then
  SPARK_MASTER_HOST=$(hostname -I)
fi

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
  --host $SPARK_MASTER_HOST \
  --port $SPARK_MASTER_PORT \
  --webui-port $SPARK_MASTER_WEBUI_PORT
