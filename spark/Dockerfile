FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY ./jars/postgresql-42.7.4.jar /opt/spark/jars/

COPY ./jobs/app.py /opt/spark/app/

COPY requirement_spark.txt /requirement_spark.txt
RUN pip install -r /requirement_spark.txt

WORKDIR /opt/spark/app

# CMD mặc định để chạy Spark ứng dụng Python
# CMD ["/opt/bitnami/scripts/spark/entrypoint.sh"]
