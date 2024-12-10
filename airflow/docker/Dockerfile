FROM apache/airflow:2.10.3-python3.12


USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean &&\
    apt-get update && apt-get install -y procps
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

RUN pip install --upgrade apache-airflow-providers-openlineage>=1.8.0