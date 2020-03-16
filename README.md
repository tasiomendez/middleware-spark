# Processing Data with Spark

The goal of this project is to infer qualitative data regarding the car accidents in New York City. The given dataset includes information
about date, time, location, borough, number of people injured and killed based (pedestrians, cyclist, motorist...), contributing factors
and vehicle types.

- Q1. Number of lethal accidents per week throughout the entire dataset.

- Q2. Number of accidents and percentage of number of deaths per contributing factor in the dataset.

  *I.e., for each contributing factor, we want to know how many accidents were due to that contributing factor and what percentage of these accidents were also lethal.*

- Q3. Number of accidents and average number of lethal accidents per week per borough.

  *I.e., for each borough, we want to know how many accidents there were in that borough each week, as well as the average number of lethal accidents that the borough had per week.*

The version of the deployments are 2.12 for Scala and 2.4.4 for Spark. All of them
can be obtained in the [official webpage](https://spark.apache.org/downloads.html).
The dataset used for this project can be downloaded from [Kaggle](https://www.kaggle.com/new-york-city/nypd-motor-vehicle-collisions).

## Usage

```shell
usage: CarAccidents [-c] [-f <FILE>] [-m <ADDRESS>] -q <QUESTION> [-s <X>]
 -c,--cached                cached dataframes on spark slaves
 -f,--file <FILE>           CSV file
 -m,--master <ADDRESS>      spark master address
 -q,--question <QUESTION>   data to access
 -s,--show <X>              show X results
```

## Deployment

The deployment is made using [Docker](https://docs.docker.com/engine/docker-overview/) and [Docker Compose](https://docs.docker.com/compose/). Spark provides different modes to run on clusters: local, client and cluster mode.

- Local mode. Spark is run locally on one machine.
- Client mode. The driver is launched in the same process as the client that submits the application.
- Cluster mode. The driver is launched from one of the worker processes inside the cluster, and the client process exits as soon as it fulfills its resposibility of submitting the application without waiting for the aplication to finish.

In this case, we provide a Spark Cluster in client mode using Spark's Standalone mode. Spark's standalone mode offers a web-based user interface to monitor the cluster. The master and each worker has its own web UI that shows cluster and job statistics.

### Getting started

The folder `docker` contains all the docker images which are needed to run. The image `base` contains is used for building the remainder images, it contains all the necessary dependencies and Spark.

```shell
cd docker
sh build.sh base      # Build base image
sh build.sh master    # Build master image
sh build.sh worker    # Build worker image
sh build.sh hadoop    # Build hadoop image
```

To deploy a simple Spark standalone cluster, run the following command.

```shell
docker-compose up spark-master spark-worker hadoop
```

For scaling the number of workers, the following command will setup one master and `X` workers.

```shell
docker-compose up spark-master spark-worker hadoop --scale spark-worker=X
```

### Submitting jobs

The image `submit` provides a template for submitting jobs to Spark in a simple way. Thus, for building the image which will submit the job to the Spark's Standalone cluster, run the following commands.

```shell
# Build images
cd docker && sh build.sh submit
docker build -t middleware/spark-app:latest .
```

```shell
# Submit job
docker-compose up spark-app
```

### Deployment wihtout Compose

For running spark without Docker Compose, i.e. running images in different machines, the following commands allow to initialize the containers.

**Spark Master**

```shell
docker run \
  --name spark-master \
  --hostname spark-master \
  --env SPARK_PUBLIC_DNS=<IPv4 address (e.g., 192.168.99.103)> \
  -p 7077:7077 \
  -p 8080:8080 \  
  middleware/spark-master:2.4.4-hadoop2.7
```

**Spark Worker**

```shell
docker run \
  --name spark-worker \
  --hostname spark-worker \
  --env SPARK_MASTER_HOST=<IPv4 address (e.g., 192.168.99.103)> \
  --env SPARK_PUBLIC_DNS=<IPv4 address (e.g., 192.168.99.103)> \
  -p 8081:8081 \
  middleware/spark-worker:2.4.4-hadoop2.7
```

**Hadoop**

```shell
docker run \
  --name hadoop \
  --hostname hadoop \
  --env HADOOP_HOST=hadoop \
  --env HADOOP_PORT=9000 \
  -p 50070:50070 \
  -p 9000:9000 \
  middleware/spark-hadoop:2.4.4-hadoop2.7
```

**Spark Submit**

```shell
docker run \
  --name spark-app \
  --hostname spark-app \
  --env SPARK_MASTER_HOST=<IPv4 address (e.g., 192.168.99.103)> \
  --env SPARK_MASTER_PORT=7077 \
  --env JAVA_PROJECT_DATASET=files/NYPD_Motor_Vehicle_Collisions.csv \
  --env HADOOP_HOST=<IPv4 address (e.g., 192.168.99.103)> \
  --env HADOOP_PORT=9000 \
  middleware/spark-app:latest \
  target/<JAR NAME>.jar \
  --question q3 q3mean --show 30
```
