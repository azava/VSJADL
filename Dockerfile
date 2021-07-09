FROM ubuntu

RUN mkdir -p /app/spark
WORKDIR /app/spark

ADD main.py /app/spark

RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install openjdk-11-jdk-headless -y
RUN apt-get install apt-utils -y
RUN apt-get install wget -y
RUN wget https://downloads.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz
RUN tar -xvzf spark-3.0.3-bin-hadoop2.7.tgz
RUN rm spark-3.0.3-bin-hadoop2.7.tgz
RUN mv spark-3.0.3-bin-hadoop2.7 /app/spark/.spark
RUN echo 'export JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home') \n\
export JAVA_HOME=${JAVA_HOME:16} \n\
export SPARK_HOME=/app/spark/.spark \n\
export PATH=$PATH:$SPARK_HOME/bin \n\
export PYTHONPATH=$SPARK_HOME/python:PYTHONPATH \n\
export PYSPARK_PYTHON=python3 \n\
export PATH=$PATH:$JAVA_HOME/jre/bin' >> pyspark_env
