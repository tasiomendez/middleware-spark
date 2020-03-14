FROM middleware/spark-base:2.4.4-hadoop2.7

RUN mkdir -p /usr/spark-conf
COPY spark-defaults.conf /usr/spark-conf


ENV SPARK_CONF_DIR /usr/spark-conf

ENV SPARK_PUBLIC_DNS localhost

EXPOSE 7001 7002 7003 7004 7005 7077 6066

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]
