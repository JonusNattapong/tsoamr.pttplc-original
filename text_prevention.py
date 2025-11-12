import cx_Oracle

# ข้อมูลการเชื่อมต่อฐานข้อมูล
USERNAME = "PTT_PIVOT"
PASSWORD = "PTT_PIVOT"
HOSTNAME = "10.100.56.3"
PORT = "1521"
SERVICE_NAME = "PTTAMR_MST"
TABLE_NAME = "AMR_CONFIGURED_DATA"

def connect_oracle():
    """เชื่อมต่อฐานข้อมูล Oracle"""
    try:
        dsn = cx_Oracle.makedsn(HOSTNAME, PORT, service_name=SERVICE_NAME)
        connection = cx_Oracle.connect(USERNAME, PASSWORD, dsn)
        return connection
    except cx_Oracle.Error as e:
        print(f"❌ Oracle Error: {e}")
        return None

def update_config_columns():
    """อัปเดตเฉพาะค่าที่ไม่สามารถแปลงเป็นตัวเลขให้เป็น NULL"""
    
    # สร้าง SQL สำหรับอัปเดต CONFIG1 - CONFIG20
    columns = [f"AMR_CONFIG{i}" for i in range(1, 21)]  # CONFIG1 - CONFIG20
    set_statements = ", ".join(
        [f"{col} = CASE WHEN REGEXP_LIKE({col}, '^-?\d+(\.\d+)?$') THEN {col} ELSE NULL END" for col in columns]
    )

    update_sql = f"""
        UPDATE {TABLE_NAME}
        SET {set_statements}
        WHERE METER_ID = 'MET0286'
        AND TRUNC(DATA_DATE) = TRUNC(SYSDATE)
    """

    connection = connect_oracle()
    if not connection:
        return
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(update_sql)
            connection.commit()
            print("✅ อัปเดต CONFIG1 - CONFIG20 สำเร็จ! (เฉพาะข้อมูลวันนี้ และ METER_ID = 'MET0286')")
    except cx_Oracle.Error as e:
        print(f"❌ Error อัปเดต: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    update_config_columns()
