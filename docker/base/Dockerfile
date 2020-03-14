FROM debian:stretch

RUN apt-get update \
     && apt-get install -y curl python3 python3-setuptools wget \
     && ln -s /usr/bin/python3 /usr/bin/python \
     && easy_install3 pip py4j \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

# JAVA
RUN apt-get update \
     && apt-get install -y default-jdk \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

# SPARK
ENV SPARK_VERSION 2.4.4
ENV HADOOP_VERSION 2.7
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV PATH $PATH:${SPARK_HOME}/bin

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz \
     && tar -xvzf ${SPARK_PACKAGE}.tgz \
     && mv ${SPARK_PACKAGE} ${SPARK_HOME} \
     && rm ${SPARK_PACKAGE}.tgz \
     && chown -R root:root $SPARK_HOME
