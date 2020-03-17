#! /bin/bash
# Starts a slave on the machine this script is executed on.

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "$SPARK_HOME/sbin/spark-config.sh"

. "$SPARK_HOME/bin/load-spark-env.sh"

if [ "$SPARK_WORKER_HOST" = "" ]; then
  SPARK_WORKER_HOST=$(hostname -I)
fi

if [ "$SPARK_DRIVER_HOST" = "" ]; then
  SPARK_DRIVER_HOST=$(hostname -I)
fi

sed -i "s|{{SPARK_DRIVER_HOST}}|$SPARK_DRIVER_HOST|g" $SPARK_CONF_DIR/spark-defaults.conf

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --host $SPARK_WORKER_HOST \
  --webui-port $SPARK_WORKER_WEBUI_PORT \
  spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
