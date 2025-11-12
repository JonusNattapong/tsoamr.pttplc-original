import cx_Oracle

username = "PTT_PIVOT"
password = "PTT_PIVOT"
hostname = "10.100.56.3"
port = "1521"
service_name = "PTTAMR_MST"

# สร้าง connection pool
connection_info = {
    "user": username,
    "password": password,
    "dsn": cx_Oracle.makedsn(hostname, port, service_name=service_name),
    "min": 1,
    "max": 5,
    "increment": 1,
    "threaded": True
}
connection_pool = cx_Oracle.SessionPool(**connection_info)

# สร้างคำสั่ง GRANT
grant_update = """GRANT UPDATE ON AMR_FIELD_METER TO PTT_PIVOT"""

# สร้าง connection จาก connection pool
with connection_pool.acquire() as connection:
    with connection.cursor() as cursor:
        cursor.execute(grant_update)
        connection.commit()
print("successfully")