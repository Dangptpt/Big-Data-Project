# Base Image
FROM apache/airflow:2.7.1-python3.11

# Switch to root user for system-level installations
USER root

# Update and install necessary packages
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean &&\
    apt-get update && apt-get install -y procps
# Set Java home environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
# Switch back to airflow user for Python package installations
USER airflow

# Upgrade OpenLineage provider to compatible version
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Install additional Airflow providers and PySpark
RUN pip install --upgrade apache-airflow-providers-openlineage>=1.8.0