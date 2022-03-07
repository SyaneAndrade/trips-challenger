FROM apache/airflow:2.2.4

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y wget && \
    apt-get clean


RUN cd "/opt" && \
    wget --no-verbose "https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz" && \
    tar xzvf "spark-3.2.1-bin-hadoop3.2.tgz" && \
    rm "spark-3.2.1-bin-hadoop3.2.tgz"


# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
ENV SPARK_HOME /opt/spark-3.2.1-bin-hadoop3.2
RUN export JAVA_HOME
RUN export SPARK_HOME

COPY airflow.cfg /opt/airflow/airflow.cfg

USER airflow