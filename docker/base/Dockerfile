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

ENV JAVA_HOME /usr/lib/jvm/default-java

# SPARK
ENV SPARK_VERSION 2.4.4
ENV HADOOP_VERSION 2.7
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV PATH $PATH:${SPARK_HOME}/bin

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz \
     && tar -xzf ${SPARK_PACKAGE}.tgz \
     && mv ${SPARK_PACKAGE} ${SPARK_HOME} \
     && rm ${SPARK_PACKAGE}.tgz \
     && chown -R root:root $SPARK_HOME

# HADOOP
ENV HADOOP_VERSION 2.7.7
ENV HADOOP_HOME /usr/hadoop-${HADOOP_VERSION}
ENV HADOOP_PACKAGE hadoop-${HADOOP_VERSION}

RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}.tar.gz \
     && tar -xzf ${HADOOP_PACKAGE}.tar.gz \
     && mv ${HADOOP_PACKAGE} ${HADOOP_HOME} \
     && rm ${HADOOP_PACKAGE}.tar.gz \
     && chown -R root:root $HADOOP_HOME
