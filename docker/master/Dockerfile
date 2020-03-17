FROM middleware/spark-base:2.4.4-hadoop2.7

ENV SPARK_PUBLIC_DNS localhost
ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080

EXPOSE 6066 7077 8080

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]
