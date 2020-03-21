FROM tasiomendez/spark-submit:2.4.4-hadoop2.7.7

ENV JAVA_PROJECT_DATASET files/dataset.csv

COPY entrypoint.sh /usr/local/bin
ENTRYPOINT ["entrypoint.sh"]

CMD ["target/application.jar"]
