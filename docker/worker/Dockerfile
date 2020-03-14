FROM middleware/spark-base:2.4.4-hadoop2.7

RUN mkdir -p /usr/spark-conf
COPY spark-defaults.conf /usr/spark-conf

ENV SPARK_CONF_DIR=/usr/spark-conf

ENV SPARK_MASTER_HOST localhost
ENV SPARK_MASTER_PORT 7077
ENV SPARK_PUBLIC_DNS localhost

ENV SPARK_WORKER_CORES 1
ENV SPARK_WORKER_MEMORY 1g
ENV SPARK_WORKER_PORT 8881
ENV SPARK_WORKER_WEBUI_PORT 8081

EXPOSE 7012 7013 7014 7015 8881

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]
