import datetime
import math
def is_valid_date(date_str, date_format='%d-%m-%Y'):
    try:
        # พยายามแปลงค่าที่ได้รับเป็น datetime object ตามรูปแบบที่กำหนด
        datetime.datetime.strptime(date_str, date_format)
        return True  # ถ้าแปลงได้สำเร็จ แสดงว่าเป็นค่าที่ถูกต้อง
    except ValueError:
        return False  # ถ้าเกิด ValueError แสดงว่าไม่ใช่ค่าที่ถูกต้อง
    

def verifyNumericReturnNULL(value):
    if (isinstance(value, (int, float)) and math.isnan(value)) or value == '' or value == 'None' or value == 'NONE' or value is None:
        return 'NULL' 
    return value