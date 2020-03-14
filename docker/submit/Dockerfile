FROM middleware/spark-base:2.4.4-hadoop2.7

RUN mkdir -p /usr/spark-conf
COPY spark-defaults.conf /usr/spark-conf

ENV SPARK_CONF_DIR /usr/spark-conf

ENV SPARK_MASTER_HOST localhost
ENV SPARK_MASTER_PORT 7077

# MAVEN
RUN apt-get update \
     && apt-get install -y maven \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/* \
     && mkdir -p /usr/src/app

# Copy the POM-file first, for separate dependency resolving and downloading
# ONBUILD COPY pom.xml /usr/src/app
# ONBUILD RUN cd /usr/src/app \
#      && mvn dependency:resolve
# ONBUILD RUN cd /usr/src/app \
#      && mvn verify

# Copy the source code and build the application
ONBUILD COPY . /usr/src/app
ONBUILD RUN cd /usr/src/app \
     && mvn clean package

ONBUILD WORKDIR /usr/src/app
