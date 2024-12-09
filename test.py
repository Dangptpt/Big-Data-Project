import psycopg2

# Thông tin kết nối
HOST = "localhost"      # Địa chỉ server PostgreSQL
DATABASE = "crime"    # Tên database
USER = "admin"      # Tên user
PASSWORD = "admin"  # Mật khẩu

try:
    # Kết nối tới PostgreSQL
    connection = psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )
    print("Kết nối PostgreSQL thành công!")
    
    # Tạo con trỏ để thực thi câu lệnh SQL
    cursor = connection.cursor()

    # Ví dụ: Lấy dữ liệu từ một bảng
    query = "SELECT * FROM premise LIMIT 5;"
    cursor.execute(query)
    results = cursor.fetchall()

    # Hiển thị kết quả
    for row in results:
        print(row)

except Exception as e:
    print(f"Lỗi khi kết nối PostgreSQL: {e}")

finally:
    # Đóng kết nối
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection:
        connection.close()
        print("Đã đóng kết nối PostgreSQL.")
