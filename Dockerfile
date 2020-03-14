FROM middleware/spark-submit:2.4.4-hadoop2.7

WORKDIR /usr/src/app

ENV SPARK_MASTER_HOST localhost
ENV SPARK_MASTER_PORT 7077
ENV JAVA_PROJECT_DATASET files/dataset.csv

RUN export JAVA_PROJECT_NAME=$(mvn exec:exec -Dexec.executable=echo -Dexec.args='${project.name}' -q)
RUN export JAVA_PROJECT_VERSION=$(mvn exec:exec -Dexec.executable=echo -Dexec.args='${project.version}' -q)

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]

CMD ["application.jar"]
