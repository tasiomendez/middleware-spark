#! /bin/sh

TAG=2.4.4-hadoop2.7
IMAGE=middleware/spark-$1:$TAG

cd $1
echo '>>' building $IMAGE in $(pwd)
docker build -t $IMAGE .
cd -

