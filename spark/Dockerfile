FROM bitnami/spark:latest

# Chuyển sang quyền root để cài đặt các dependencies
USER root

# Cài đặt các thư viện cần thiết
RUN apt-get update && apt-get install -y \
    python3-pip \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Python libraries
RUN pip3 install pyspark pandas

# Sao chép PostgreSQL JDBC driver
COPY ./jars/postgresql-42.7.4.jar /opt/spark/jars/

# Sao chép ứng dụng Spark (Python script)
COPY ./jobs/app.py /opt/spark/app/

COPY requirement_spark.txt /requirement_spark.txt
RUN pip install -r /requirement_spark.txt

# Thiết lập thư mục làm việc
WORKDIR /opt/spark/app

# CMD mặc định để chạy Spark ứng dụng Python
# CMD ["/opt/bitnami/scripts/spark/entrypoint.sh"]
