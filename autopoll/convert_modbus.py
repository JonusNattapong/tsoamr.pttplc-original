import struct
import datetime
import math
#from datetime import datetime

def unpack_float(raw_data):
    # ต้องได้ 4 bytes สำหรับ float
    hex_str = raw_data.zfill(8)  # 8 hex digits = 4 bytes
    return struct.unpack('!f', bytes.fromhex(hex_str))[0]

def unpack_double(raw_data):
    # ต้องได้ 8 bytes สำหรับ double
    hex_str = raw_data.zfill(16)  # 16 hex digits = 8 bytes
    return struct.unpack('!d', bytes.fromhex(hex_str))[0]


def convert_raw_to_value(data_type, raw_data, mode="normal"):
    """
    Converts raw data to a specific format based on data type and mode.

    Parameters:
        data_type (str): The type of data to process (e.g., "Date", "Float", "Ulong").
        raw_data (str): The raw hexadecimal data to be converted.
        mode (str): The mode of operation ("billing", "normal", "hourly").

    Returns:
        str/float/int: The processed value based on data type and mode.
    """
    try:
        if mode == "billing":
            if data_type == "Date":
                try:
                    raw_data_as_int = int(raw_data, 16)
                    date_object = datetime.datetime.fromtimestamp(raw_data_as_int).date()
                    modified_date = date_object - datetime.timedelta(days=1)
                    formatted_date = modified_date.strftime('%d-%m-%Y')
                    return formatted_date
                except (ValueError, OverflowError):
                    return "0"
            
            elif data_type == "EVODate":
                try:
                    if len(raw_data) != 12:
                        return raw_data
                    
                    year = int(raw_data[:2]) + 2000
                    month = int(raw_data[2:4])
                    day = int(raw_data[4:6])
                    hour = int(raw_data[6:8])
                    minute = int(raw_data[8:10])
                    second = int(raw_data[10:12])

                    if not (1 <= month <= 12) or not (1 <= day <= 31) or not (0 <= hour < 24) or not (0 <= minute < 60) or not (0 <= second < 60):
                        return raw_data

                    date_object = datetime.datetime(year, month, day, hour, minute, second)
                    formatted_date = date_object.strftime('%d-%m-%Y %H:%M:%S')
                    formatted_date = date_object.strftime('%d-%m-%Y')
                    return formatted_date
                except (ValueError, OverflowError):
                    return "0"

        elif mode == "hourly":
            if data_type == "Hour":
                raw_data_as_int = int(raw_data, 16)
                date_object = datetime.datetime.utcfromtimestamp(raw_data_as_int)
                formatted_date = date_object.strftime('%d-%m-%Y on %H:00')
                return formatted_date

        # Normal and shared processing
        if data_type == "Date":
            raw_data_as_int = int(raw_data, 16)
            date_object = datetime.datetime.utcfromtimestamp(raw_data_as_int)
            formatted_date = date_object.strftime('%d-%m-%Y %H:%M:%S')
            return formatted_date
        elif data_type == "EVODate":
            try:
                if len(raw_data) != 12:
                    return raw_data
                
                year = int(raw_data[:2]) + 2000
                month = int(raw_data[2:4])
                day = int(raw_data[4:6])
                hour = int(raw_data[6:8])
                minute = int(raw_data[8:10])
                second = int(raw_data[10:12])

                if not (1 <= month <= 12) or not (1 <= day <= 31) or not (0 <= hour < 24) or not (0 <= minute < 60) or not (0 <= second < 60):
                    return raw_data

                date_object = datetime.datetime(year, month, day, hour, minute, second)
                formatted_date = date_object.strftime('%d-%m-%Y %H:%M:%S')
                #formatted_date = date_object.strftime('%d-%m-%Y')
                return formatted_date
            except (ValueError, OverflowError):
                return "0"

        elif data_type == "Float":
            # try:
            #     float_value = struct.unpack('!f', bytes.fromhex(raw_data))[0]
            #     rounded_float_value = round(float_value, 5)
            #     return rounded_float_value
            # except struct.error:
            #     return ''
            try:
                #float_value = struct.unpack('!f', bytes.fromhex(raw_data))[0]
                float_value = unpack_float(raw_data)
                if not math.isfinite(float_value):  # ตรวจสอบว่าไม่ใช่ inf หรือ NaN
                    return ''
                rounded_float_value = round(float_value, 5)
                return rounded_float_value
            except (struct.error, ValueError):  # เพิ่ม ValueError เพื่อดักจับ bytes ที่ไม่ถูกต้อง
                return ''

        elif data_type == "Double":
            # try:
            #     double_value = struct.unpack('!d', bytes.fromhex(raw_data))[0]
            #     rounded_double_value = round(double_value, 5)
            #     return rounded_double_value
            # except struct.error:
            #     return ''
            try:
                #double_value = struct.unpack('!d', bytes.fromhex(raw_data))[0]
                double_value = unpack_double(raw_data)
                if not math.isfinite(double_value):  # ตรวจสอบว่าไม่ใช่ inf หรือ NaN
                    return ''
                rounded_double_value = round(double_value, 5)
                return rounded_double_value
            except (struct.error, ValueError):  # เพิ่ม ValueError เพื่อดักจับ bytes ที่ไม่ถูกต้อง
                return ''

        elif data_type == "Ulong":
            try:
                return int(raw_data, 16)
            except ValueError:
                return ''

        else:
            return raw_data

    except Exception as e:
        return ''

