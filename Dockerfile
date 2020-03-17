FROM middleware/spark-submit:2.4.4-hadoop2.7

WORKDIR /usr/src/app

ENV SPARK_MASTER_HOST master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_DEPLOY_MODE client

ENV HADOOP_HOST hadoop
ENV HADOOP_PORT 9000

ENV JAVA_JAR_MAIN_CLASS my.main.Application
ENV JAVA_PROJECT_DATASET files/dataset.csv

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]

CMD ["target/application.jar"]
