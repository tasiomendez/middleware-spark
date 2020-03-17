FROM middleware/spark-base:2.4.4-hadoop2.7

COPY spark-defaults.conf .
RUN mv spark-defaults.conf $SPARK_HOME/conf/

ENV SPARK_PUBLIC_DNS localhost
ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080

EXPOSE 7001 7002 7003 7004 7005 7077 6066 8080

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]
