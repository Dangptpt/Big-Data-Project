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


```
docker-compose up

docker exec -it spark-master bash -c "pip install numpy" 

docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.7.4.jar /opt/spark/app/app.py"