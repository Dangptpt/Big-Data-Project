# Big-Data-Project

Môn học: Lưu trữ và xử lý dữ liệu lớn (IT4931).

Nhóm: 10

| STT | Họ và Tên               | Mã Số Sinh Viên  |
|-----|-------------------------|------------------|
|  1  | Trần Duy Mẫn            |      20210566    |
|  2  | Hà Đức Chung            |      20215322    |
|  3  | Phạm Tiến Duy           |      20215335    |
|  4  | Phùng Thanh Đăng        |      20210150    |
|  5  | Nguyễn Trình Tuấn Đạt   |      20210177    |


1. **First run**
```
docker-compose up --build -d
```
2. **To send data to HDFS you can do any of two ways below**
* Assess localhost 8080 to send data to HDFS using fast API docs
* access (fill date you want to send data to HDFS): http://localhost:8000/send_data_by_date?date={**date**}

3. **RUN spark to process data then send to Postgres DB**
```
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.7.4.jar /opt/spark/app/app.py"
```