
import pandas as pd
import cx_Oracle
from flask import flash
import traceback
from datetime import datetime
from flask import abort
import socket
import struct
from pymodbus.utilities import computeCRC
import time
import datetime
import logging
from collections import Counter
communication_traffic = []
change_to_32bit_counter = 0 
def convert_to_binary_string(value, bytes_per_value):
    binary_string = bin(value)[
        2:
    ]  
    return binary_string.zfill(
        bytes_per_value * 8
    )  

def convert_raw_to_value_config(data_type, raw_data):
    if data_type == "Date":
        raw_data_as_int = int(raw_data, 16)
        date_object = datetime.datetime.fromtimestamp(raw_data_as_int).date()
        
        # No need to subtract days from the date_object
        # modified_date = date_object - datetime.timedelta(days=1)
        
        formatted_date = date_object.strftime('%d-%m-%Y')
        
        return formatted_date
    elif data_type == "Float":
        float_value = struct.unpack('!f', bytes.fromhex(raw_data))[0]
        rounded_float_value = round(float_value, 5)  
        return rounded_float_value
    elif data_type == "Ulong":
        return int(raw_data, 16)
    else:
       
        return raw_data

def convert_raw_to_value(data_type, raw_data):
    # if data_type == "Date":
    #     raw_data_as_int = int(raw_data, 16)
    #     date_object = datetime.datetime.fromtimestamp(raw_data_as_int).date()
        
    #     formatted_date = date_object.strftime('%d-%m-%Y') 
        
    #     return formatted_date
    if data_type == "Date":
        raw_data_as_int = int(raw_data, 16)
        date_object = datetime.datetime.fromtimestamp(raw_data_as_int).date()
        
        
        modified_date = date_object - datetime.timedelta(days=1)
        
        formatted_date = modified_date.strftime('%d-%m-%Y') 
        
        return formatted_date
    elif data_type == "Float":
        float_value = struct.unpack('!f', bytes.fromhex(raw_data))[0]
        rounded_float_value = round(float_value, 5)  
        return rounded_float_value
    elif data_type == "Ulong":
        return int(raw_data, 16)
    else:
       
        return raw_data 
############  connect database  #####################

# username = "root"
# password = "root"
# hostname = "192.168.102.192"
# port = "1521"
# service_name = "orcl"


username = "PTT_PIVOT"
password = "PTT_PIVOT"
hostname = "10.100.56.3"
port = "1521"
service_name = "PTTAMR_MST"
dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

# สร้าง connection pool ที่ใช้ในการเชื่อมต่อแบบประหยัดทรัพยากร
connection_info = {
    "user": username,
    "password": password,
    "dsn": dsn,
    "min": 1,
    "max": 5,
    "increment": 1,
    "threaded": True
}
connection_pool = cx_Oracle.SessionPool(**connection_info)

# อ่านข้อมูลจากฐานข้อมูลโดยใช้ connection pool
def fetch_data(query, params=None):
    try:
        with connection_pool.acquire() as connection:
            with connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                results = cursor.fetchall()
        return results
    except cx_Oracle.Error as e:
        (error,) = e.args
        print("Oracle Error:", error)
        return []


logger_error = logging.getLogger('error_logger')
logger_error.setLevel(logging.ERROR)

# สร้าง Handler เพื่อบันทึกข้อความ error ลงในไฟล์
log_file_error = 'C:\\Users\\Administrator\\Desktop\\autopoll\\Autopoll_repeat1\\Actaris(u).log'
file_handler_error = logging.FileHandler(log_file_error)
formatter_error = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
file_handler_error.setFormatter(formatter_error)
logger_error.addHandler(file_handler_error)
query = """
             SELECT
                    AMR_PL_GROUP.PL_REGION_ID as region,
                    AMR_FIELD_ID.TAG_ID as Sitename,
                     AMR_FIELD_METER.METER_NO_STREAM as NoRun,
                    
                    AMR_FIELD_METER.METER_STREAM_NO as RunNo,
                    
                    AMR_FIELD_METER.METER_ID as METERID,
                
                    AMR_VC_TYPE.VC_NAME as VCtype,
                    
                    AMR_FIELD_ID.SIM_IP as IPAddress,
                    AMR_PORT_INFO.PORT_NO as port,
                    
                     
                    AMR_POLL_RANGE.poll_config as poll_config,
                    AMR_POLL_RANGE.poll_billing as poll_billing,
                    AMR_POLL_RANGE.POLL_CONFIG_ENABLE as POLL_CONFIG_ENABLE,
                    AMR_POLL_RANGE.POLL_BILLING_ENABLE as POLL_BILLING_ENABLE,
                    AMR_VC_TYPE.id as evctype,
                    amr_field_meter.modbus_id as modbus_id
                FROM
                    AMR_POLL_RANGE,
                    AMR_FIELD_ID,
                    AMR_USER,
                    AMR_FIELD_CUSTOMER,
                    AMR_FIELD_METER,
                    AMR_PL_GROUP,
                    AMR_VC_TYPE,
                    AMR_PORT_INFO
                WHERE
                    AMR_FIELD_METER.METER_AUTO_ENABLE=1 AND
                    AMR_FIELD_ID.FIELD_ID = AMR_PL_GROUP.FIELD_ID AND
                    AMR_FIELD_ID.METER_ID = AMR_USER.USER_GROUP AND
                    AMR_FIELD_ID.CUST_ID = AMR_FIELD_CUSTOMER.CUST_ID AND
                    AMR_FIELD_ID.METER_ID = AMR_FIELD_METER.METER_ID AND
                    AMR_VC_TYPE.ID = AMR_FIELD_METER.METER_STREAM_TYPE AND
                    AMR_FIELD_METER.METER_PORT_NO = AMR_PORT_INFO.ID AND
                    amr_poll_range.evc_type = AMR_VC_TYPE.id AND
                    amr_vc_type.id like '12' 
                                   
                ORDER BY
                                        
                                        AMR_FIELD_ID.TAG_ID ASC, port
                                        
                                        
                                        
                
"""



rows =  fetch_data(query)
# print(rows)

successful_message_test= []
error_message_test = []
completed_sets = 0
successful_rows = 0
error_count = 0
for row in rows:
    try:
        Sitename = row[1]
      
       
        print("Actaris U")
        tcp_ip = row[6]
        print("tcp_ip",tcp_ip)
        tcp_port = row[7]
        print("tcp_port",tcp_port)
        run = row[3]
        print("run",run)
        METERID = row[4]
        print("METERID",METERID)
        poll_config_set = row[8]
        # print("poll_config_set",poll_config_set)
        poll_billing_set = row[9]
        # print("poll_billing_set",poll_billing_set)
        CONFIG_ENABLE_set = row[10].replace(',', '')
        # print("CONFIG_ENABLE_set",CONFIG_ENABLE_set)
        BILLING_ENABLE_set = row[11].replace(',', '')
        # print("BILLING_ENABLE_set",BILLING_ENABLE_set)
        
       
        
        
        evc_type = row[12]
        print("evc_type",evc_type)
        modbus_id =row[13]
        print("modbus_id",modbus_id)
#      
        function_code = 3
        completed_sets += 1
        starting_address_config_ = {}
        quantity_config_ = {}
        starting_address_ = {}
        quantity_ = {}


        for i, value in enumerate(poll_config_set.split(',')):
            if i % 2 == 0:
                starting_address_config_[i // 2 + 1] = int(value)
                
            else:
                quantity_config_[i // 2 + 1] = int(value)
                
        for i, value in enumerate(poll_billing_set.split(',')):
            if i % 2 == 0:
                starting_address_[i // 2 + 1] = int(value)
                
                
            else:
                quantity_[i // 2 + 1] = int(value)
        
        
        

        data= {'starting_address_i': [], 
            'quantity_i': [], 
            'adjusted_quantity_i': []}
        df_pollRange = pd.DataFrame(data)
        df_pollBilling = pd.DataFrame(data)
        for i in range(1, 6):
            if CONFIG_ENABLE_set[i-1] == '1':
                starting_address_i = starting_address_config_[i]
                quantity_i = quantity_config_[i]
                adjusted_quantity_i = quantity_i - starting_address_i + 1
                data = {'starting_address_i': [starting_address_i], 
                        'quantity_i': [quantity_i], 
                        'adjusted_quantity_i': [adjusted_quantity_i]}
                df_2 = pd.DataFrame(data)
                
                df_pollRange = pd.concat([df_pollRange,df_2] , ignore_index=True)
        
        for i in range(1, 11): 
            
            if BILLING_ENABLE_set[i-1] == '1': 
                
                starting_address_i = starting_address_[i]
                
                quantity_i =  quantity_[i]
                
                adjusted_quantity_i = quantity_i - starting_address_i + 1
                data= {'starting_address_i': [starting_address_i], 
                    'quantity_i': [quantity_i], 
                    'adjusted_quantity_i': [adjusted_quantity_i]}
                
                df_2 = pd.DataFrame(data)
                df_pollBilling = pd.concat([df_pollBilling,df_2] , ignore_index=True)
    



        sock_i = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_i.settimeout(20)

        
        sock_i.connect((tcp_ip, int(tcp_port)))
        print("Connected successfully")
        
            
        dataframes = {
                'address_start': [],
                'finish': [],
                'TX': [],
                'RX': []
            }
        df_Modbus = pd.DataFrame(dataframes)
        df_Modbusbilling = pd.DataFrame(dataframes)
       
        slave_id_1 = 0x01
        function_code_1 = 0x03
        starting_address_1 = 0x0004
        quantity_1 = 0x0002

        request_Actaris= bytearray([
            slave_id_1,
            function_code_1,
            (starting_address_1 >> 8) & 0xFF,
            starting_address_1 & 0xFF,
            (quantity_1 >> 8) & 0xFF,
            quantity_1 & 0xFF,
        ])

        
        crc_1 = computeCRC(request_Actaris)
        request_Actaris += crc_1.to_bytes(2, byteorder="big") 


        if int(tcp_port) in [2402, 2404,4003]:
        # #send wa
            for _ in range(2):  
                sock_i.send(request_Actaris)
                print("if tcp_port",sock_i)
                time.sleep(1)
            response = sock_i.recv(4096)
        
        for i in range(0, len(df_pollRange)):
            
            if int(tcp_port) in [2101]:
                sock_i = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_i.settimeout(20)

                sock_i.connect((tcp_ip, int(tcp_port)))
                # print("Connected successfully")


            start_address = int(df_pollRange.loc[i,'starting_address_i'])
            
            adjusted_quantity = int(df_pollRange.loc[i,'adjusted_quantity_i'])
            # print(start_address,adjusted_quantity)
            

           
        
           
            request_message_i = bytearray(
            [modbus_id, function_code, start_address >> 8, start_address & 0xFF, adjusted_quantity >> 8, adjusted_quantity & 0xFF])
            crc_i = computeCRC(request_message_i)
            request_message_i += crc_i.to_bytes(2, byteorder="big")
            # print(request_message_i)
            communication_traffic_i = []
        
            
            communication_traffic_i.append(request_message_i.hex())
            print("TX.config",communication_traffic_i[0])
            
            time.sleep(2)

            sock_i.send(request_message_i)
            
            
            response_i = sock_i.recv(1024)
            
            if response_i:
                communication_traffic_i.append(response_i.hex())
                print("RX.config", communication_traffic_i[1])
            else:
                print("No response received")
                communication_traffic_i.append("No response")
            


            
            if response_i[1:2] != b'\x03':
                    abort(400, f"Error: Unexpected response code from device {communication_traffic_i[1]}!")
            else:
                    pass
            # sock_i.close()
            
                        
            data = {
                'address_start': [int(start_address)],
                'finish': [int(start_address+adjusted_quantity)],
                'TX': [communication_traffic_i[0]],
                'RX': [communication_traffic_i[1]]
            }
            
            df_2 = pd.DataFrame(data)
            df_Modbus = pd.concat([df_Modbus, df_2], ignore_index=True)

            # print(df_Modbus)



        for i in range(0, len(df_pollBilling)):
            if int(tcp_port) in [2101]:
            
                sock_i = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_i.settimeout(20)
                
                sock_i.connect((tcp_ip, int(tcp_port)))
                # print("Connected successfully")

            start_address = int(df_pollBilling.loc[i,'starting_address_i'])
            adjusted_quantity = int(df_pollBilling.loc[i,'adjusted_quantity_i'])
        
            request_message_i = bytearray(
            [modbus_id, function_code, start_address >> 8, start_address & 0xFF, adjusted_quantity >> 8, adjusted_quantity & 0xFF])
            crc_i = computeCRC(request_message_i)
            request_message_i += crc_i.to_bytes(2, byteorder="big")
            
            # print(request_message_i)
            communication_traffic_i = []
            
            communication_traffic_i.append(request_message_i.hex())
            print("TX.billing",communication_traffic_i[0])
            
            time.sleep(2)
            sock_i.send(request_message_i)
            
            response_i = sock_i.recv(4096)
            
            if response_i:
                communication_traffic_i.append(response_i.hex())
                print("RX.billing", communication_traffic_i[1])
            else:
                print("No response received")
                communication_traffic_i.append("No response")
            
            if response_i[1:2] != b'\x03':
                    abort(400, f"Error: Unexpected response code from device {communication_traffic_i[1]}!")
            else:
                    pass

            
            # print(communication_traffic_i)
            data = {
                'address_start': [int(start_address)],
                'finish': [int(start_address+adjusted_quantity-1)],
                'TX': [communication_traffic_i[0]],
                'RX': [communication_traffic_i[1]]
            }
            # print(data)
            df_2 = pd.DataFrame(data)
            df_Modbusbilling = pd.concat([df_Modbusbilling, df_2], ignore_index=True)
            
            
            # print(df_2)
            
            
            

            
            query = f"select amc.or_der as order1, amc.address as address1, amc.description as desc1, amc.data_type as dtype1 \
            from amr_mapping_config amc \
            where amc.evc_type = '{evc_type}' AND address is not null \
            order by order1"
            
            cursor = fetch_data(query)
            df_mapping = pd.DataFrame(cursor, columns=['order', 'address', 'desc', 'data_type'])
            

            list_of_values_configured = []
            for i in range(0, len(df_mapping)):
                
                address = int(df_mapping.iloc[i,1])
                
                data_type = str(df_mapping.iloc[i,3])
                
                
                for j in range(0,len(df_Modbus)):
                    address_start = int(df_Modbus.iloc[j,0])
                    address_finish = int(df_Modbus.iloc[j,1])
                    #print(address)
                    if address >= address_start and address <= address_finish:
                        # print(address_start, address_finish, df_Modbus.iloc[j,3])
                        location_data = (address - address_start)*int(8/2)
                        frameRx = (df_Modbus.iloc[j,3])
                        #
                        raw_data = frameRx[location_data + 6: location_data + 14]
                        
                    
                        list_of_values_configured.append(convert_raw_to_value_config(data_type, raw_data))
                        # print(list_of_values_configured)
                        break
            # value_config = pd.DataFrame(list_of_values_configured,columns=['Value'])
            # result_config = pd.concat([df_mapping, value_config], axis=1)
            # print(result_config)


            
            query = f"SELECT amb.daily ,amb.or_der ,amb.address,amb.description,amb.data_type  FROM amr_mapping_billing amb WHERE amb.evc_type = '{evc_type}' AND address is not null order by amb.daily,amb.or_der"
            cursor = fetch_data(query)
            
            df_mappingbilling = pd.DataFrame(cursor, columns=['daily','or_der', 'address', 'description', 'data_type'])

        

            list_of_values_billing = []
            for i in range(0, len(df_mappingbilling)):
                
                address = int(df_mappingbilling.iloc[i,2])
                
                data_type = str(df_mappingbilling.iloc[i,4])
                
                
                for j in range(0,len(df_Modbusbilling)):
                    address_start = int(df_Modbusbilling.iloc[j,0])
                    address_finish = int(df_Modbusbilling.iloc[j,1])
                
                    if address >= address_start and address <= address_finish:
                    
                        location_data = (address - address_start)*int(8/2)
                        
                        frameRx = (df_Modbusbilling.iloc[j,3])
                    
                        raw_data = frameRx[location_data + 6: location_data + 14]
                        
                        
                        list_of_values_billing.append(convert_raw_to_value(data_type,raw_data))   
                        
                        break
            
            

    
        current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime('%d-%b-%y %I.%M.%S.%f %p ASIA/BANGKOK')
        current_datetime_upper = current_datetime.upper()
        date_system = datetime.datetime.now().strftime('%d-%m-%Y')   
        
    
    
    
    
        


        
        sql_texts = []
        for i in range(0, len(df_mappingbilling), 5):
            
                
                values_subset = list_of_values_billing[i:i+5]
                # print(values_subset)
                
                # print(sql_text_billing_delete)
                sql_text_billing_insert = f"INSERT INTO AMR_BILLING_DATA (METER_ID, METER_STREAM_NO, DATA_DATE,TIME_CREATE,UNCORRECTED_VOL ,CORRECTED_VOL, AVR_TF, AVR_PF) VALUES ('{METERID}', '{run}', TO_DATE('{values_subset[0]}', 'DD-MM-YYYY'),"
                

                sql_text_billing_insert += f"'{current_datetime_upper}'"

                for value in values_subset[1:]:
                    sql_text_billing_insert += f", {value}"
                
                sql_text_billing_insert += ");"
            
                sql_text_billing_insert = sql_text_billing_insert.rstrip(',')
                
                sql_texts.append(sql_text_billing_insert)

        full_sql_text = "\n".join(sql_texts)
            
        
        print(full_sql_text)

        with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                with connection.cursor() as cursor:
                    for sql_statement in full_sql_text.split(";"):
                        if sql_statement.strip():
                            cursor.execute(sql_statement.strip())
                    connection.commit()
                    print("Insert data billing successful")







                    
        query_maxdaily = f"SELECT MAX(DAILY)FROM amr_mapping_billing WHERE evc_type = '{evc_type}'"
        result_maxdaily = fetch_data(query_maxdaily) 
        
        max_daily_value = result_maxdaily[0][0]
        print(max_daily_value)
            # print("max_daily_value",":",max_daily_value)
            
            
        query_maxdaily_1 = f"""SELECT MAX(DAILY)-1 FROM amr_mapping_billing WHERE evc_type = '{evc_type}'"""
        result_maxdaily = fetch_data(query_maxdaily_1) 
        max_daily_value_1 = result_maxdaily[0][0]
        print(max_daily_value_1)
        
        
        
        values_subset_1 = list_of_values_billing[0]
        query_maxdate = f"""SELECT DATA_DATE 
                            FROM (
                                SELECT DATA_DATE 
                                FROM amr_billing_data 
                                WHERE data_date = TO_DATE('{values_subset_1}', 'DD-MM-YYYY') 
                                AND meter_id = '{METERID}' 
                                AND meter_stream_no = '{run}' 
                            )
                            WHERE ROWNUM <= 1"""
        maxdate_db = fetch_data(query_maxdate) 
        
        maxdate_billing_1 = pd.DataFrame(maxdate_db)
        
        if maxdate_billing_1.empty:
            maxdate_billing_str_1 = "0-0-0"
            
        else:
            maxdate_billing_1 = maxdate_billing_1.iloc[0]
        
            maxdate_billing_str_1 = maxdate_billing_1.iloc[0].strftime('%d-%m-%Y')
            

        
        
        values_subset = list_of_values_billing[5]
        query_maxdate = f"""SELECT DATA_DATE 
                            FROM (
                                SELECT DATA_DATE 
                                FROM amr_billing_data 
                                WHERE data_date = TO_DATE('{values_subset}', 'DD-MM-YYYY') 
                                AND meter_id = '{METERID}' 
                                AND meter_stream_no = '{run}' 
                            )
                            WHERE ROWNUM <= 1
                            """
        maxdate_db = fetch_data(query_maxdate)
    
        maxdate_billing = pd.DataFrame(maxdate_db)
        maxdate_billing = maxdate_billing.iloc[0]
        maxdate_billing_str = maxdate_billing.iloc[0].strftime('%d-%m-%Y')
        # print(maxdate_billing_str)
        
        
        
        
        
        # # # ########### เช็คค่า poll ซ้ำ #####################
        if values_subset_1 == maxdate_billing_str_1:
                
                print("poll ซ้ำ")
                
                query = f"""SELECT  DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL 
                            FROM amr_billing_data 
                            WHERE DATA_DATE BETWEEN TO_DATE('{values_subset_1}', 'DD-MM-YYYY') - INTERVAL '{max_daily_value_1}' DAY AND TO_DATE('{values_subset_1}', 'DD-MM-YYYY') 
                            AND meter_id = '{METERID}' 
                            AND meter_stream_no = '{run}' 
                            ORDER BY DATA_DATE DESC"""
                # print(query)
                results_billing = fetch_data(query)
                print(query)
                
                df_billing = pd.DataFrame(results_billing , columns=['DATA_DATE','CORRECTED_VOL', 'UNCORRECTED_VOL'])
                df_billing['DATA_DATE'] = df_billing['DATA_DATE'].dt.strftime('%d-%m-%Y')
                # print(df_billing_1)
                df_billing['DATA_DATE'] = pd.to_datetime(df_billing['DATA_DATE'], format='%d-%m-%Y')
                date_counts = df_billing['DATA_DATE'].value_counts()

                
                single_dates = date_counts[date_counts == 1].index.tolist()

                single_dates_formatted = pd.to_datetime(single_dates).strftime('%d-%m-%Y')
                print(single_dates_formatted)
                single_date_rows = df_billing[df_billing['DATA_DATE'].isin(single_dates)]

            
                df_billing = df_billing[~df_billing['DATA_DATE'].isin(single_dates)]
               
                if len(single_dates_formatted) > 0:
                    # for นี้คือ amr_billing_data_errorเช็คamr_billing_data เเละโดยเฉพาะ
                    for date_formatted in single_dates_formatted:
                        print(date_formatted)
                        sql_text_billing_NotMatched = f"""SELECT DATA_DATE, METER_ID, METER_STREAM_NO 
                                                        FROM amr_billing_data_error
                                                        WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                                        AND METER_ID = '{METERID}' 
                                                        AND METER_STREAM_NO = '{run}'"""
                        billing_NotMatched = fetch_data(sql_text_billing_NotMatched)
                        
                       
                        sql_text_billing = f"""SELECT DATA_DATE, METER_ID, METER_STREAM_NO 
                                            FROM amr_billing_data
                                            WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                            AND METER_ID = '{METERID}' 
                                            AND METER_STREAM_NO = '{run}'"""
                        billing_data = fetch_data(sql_text_billing)
                        




                        if billing_NotMatched and billing_data:
                            print("ทั้งสองตัวแปรมีข้อมูล")

                            sql_billing = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                            FROM amr_billing_data
                                            WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                            AND METER_ID = '{METERID}' 
                                            AND METER_STREAM_NO = '{run}'"""
                            billing_data_all = fetch_data(sql_billing)
                            print("Billing Data All:", billing_data_all)

                            sql_billing_error = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data_error
                                                    WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                                    AND METER_ID = '{METERID}' 
                                                    AND METER_STREAM_NO = '{run}'"""
                            billing_data_all_error = fetch_data(sql_billing_error)
                            print("Billing Data All Error:", billing_data_all_error)

                            # Find matching records
                            billing_data_all_dicts = [{'CORRECTED_VOL': row[0], 'UNCORRECTED_VOL': row[1], 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all]
                            billing_data_all_error_dicts = [{'CORRECTED_VOL': int(row[0]), 'UNCORRECTED_VOL': int(row[1]), 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all_error]

                            # Find matching records
                            matching_records = []
                            for data in billing_data_all_dicts:
                                for error_data in billing_data_all_error_dicts:
                                    print(error_data)
                                    if (data['CORRECTED_VOL'] == error_data['CORRECTED_VOL'] and
                                        data['UNCORRECTED_VOL'] == error_data['UNCORRECTED_VOL'] and
                                        data['AVR_PF'] == error_data['AVR_PF'] and
                                        data['AVR_TF'] == error_data['AVR_TF']):
                                        matching_records.append(data)

                            if matching_records:
                                print("Matching Records:")
                                for record in matching_records:
                                    print(record)
                            else:
                                print("No matching records found.")
                                
                                sql_texts = []
                                for row in billing_data_all_dicts:
                                    formatted_avr_pf = f"{row['AVR_PF']:.5f}"
                                    formatted_avr_tf = f"{row['AVR_TF']:.2f}"

                                    sql_text_billing_insert = f"""
                                        INSERT INTO AMR_BILLING_DATA_ERROR 
                                        (METER_ID, METER_STREAM_NO, DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF,TIME_CREATE) 
                                        VALUES 
                                        ('{METERID}', '{run}', TO_DATE('{date_formatted}', 'DD-MM-YYYY'), 
                                        '{row['CORRECTED_VOL']}', '{row['UNCORRECTED_VOL']}', '{formatted_avr_pf}', '{formatted_avr_tf}','{current_datetime_upper}')
                                    """
                                    sql_texts.append(sql_text_billing_insert.strip())

                                full_sql_text = "\n".join(sql_texts)
                                print(full_sql_text)
                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(full_sql_text)
                                                        connection.commit()
                                                    print("Insert data ERROR successful")






                            sql_text_billing_NotMatched_delete = f"""DELETE FROM AMR_BILLING_DATA 
                                                                        WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY') 
                                                                        AND METER_ID = '{METERID}' 
                                                                        AND METER_STREAM_NO = '{run}'"""

                            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                    with connection.cursor() as cursor:
                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                        connection.commit()
                                        print("Deleted 'Not' Matched data from billing successfully")

                            
                                
                        else:
                            print("อย่างน้อยหนึ่งตัวแปรไม่มีข้อมูล")

                        
                           
                            
                    # `for หลังจากinsert billingตรวจเช็คข้อมูลของCORRECTED_VOLเเละUNCORRECTED_VOL ว่าเท่ากันไหม 
                    for i in range(0, len(df_billing), 2):
                            db_test_matched = df_billing[i:i+2] 
                                    # print(db_test_matched)
                            db_test_matched['DATA_DATE'] = pd.to_datetime(db_test_matched['DATA_DATE']).dt.strftime('%d-%m-%Y')
                            if db_test_matched['DATA_DATE'].nunique() == 1 and db_test_matched['CORRECTED_VOL'].nunique() == 1 and db_test_matched['UNCORRECTED_VOL'].nunique() == 1:
                                                print("Matched:", db_test_matched)
                                                TIME_CREATE_MAX = f"""SELECT MAX(TIME_CREATE) AS MAX_TIME_CREATE FROM amr_billing_data WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'"""
                                                MAX_TIME_CREATE = fetch_data(TIME_CREATE_MAX)
                                                MAX_TIME_CREATE_ALL = MAX_TIME_CREATE[0][0]
                                                
                                                sql_text_billing_matched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            AND TIME_CREATE = '{MAX_TIME_CREATE_ALL}'"""
                                                # print(sql_text_billing_matched_delete)
                                                # print(sql_text_billing_matched_delete)
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_matched_delete)
                                                        connection.commit()
                                                        print("Deleted matched data from billing successfully")  
                                            
                                            
                                            
                            else:
                                        print("Not Matched:", db_test_matched)
                                        query = f"""SELECT   DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data 
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                    AND meter_id = '{METERID}' 
                                                    AND meter_stream_no = '{run}' 
                                                    ORDER BY DATA_DATE DESC"""
                                        
                                        
                                        results_billing = fetch_data(query)
                                        sql_billing_error = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data_error
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY')
                                                    AND METER_ID = '{METERID}' 
                                                    AND METER_STREAM_NO = '{run}'"""
                                        billing_data_all_error = fetch_data(sql_billing_error)
                                        # Find matching records
                                        billing_data_all_dicts = [{'CORRECTED_VOL': row[1], 'UNCORRECTED_VOL': row[2], 'AVR_PF': float(row[3]), 'AVR_TF': float(row[4])} for row in results_billing]
                                        billing_data_all_error_dicts = [{'CORRECTED_VOL': int(row[0]), 'UNCORRECTED_VOL': int(row[1]), 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all_error]

                                        # Find matching records
                                        matching_records = []
                                        non_matching_records = []

                                        # Find matching and non-matching records
                                        for data in billing_data_all_dicts:
                                            match_found = False
                                            for error_data in billing_data_all_error_dicts:
                                                if (data['CORRECTED_VOL'] == error_data['CORRECTED_VOL'] and
                                                    data['UNCORRECTED_VOL'] == error_data['UNCORRECTED_VOL'] and
                                                    data['AVR_PF'] == error_data['AVR_PF'] and
                                                    data['AVR_TF'] == error_data['AVR_TF']):
                                                    matching_records.append(data)
                                                    match_found = True
                                                    break
                                            if not match_found:
                                                non_matching_records.append(data)

                                        # Process the matching records
                                        if matching_records:
                                            print("Matching Records:")
                                            for record in matching_records:
                                                print(record)
                                        else:
                                            print("No matching records found in the matching list.")

                                        # Process the non-matching records
                                        if non_matching_records:
                                            print("Non-matching Records:")
                                            for record in non_matching_records:
                                                # Extract values from the record
                                                print(record)
                                                corrected_vol = record['CORRECTED_VOL']
                                                uncorrected_vol = record['UNCORRECTED_VOL']
                                                
                                                formatted_avr_pf = f"{record['AVR_PF']:.5f}"
                                                formatted_avr_tf = f"{record['AVR_TF']:.2f}"
                                                sql_text_billing_insert = """
                                                    INSERT INTO AMR_BILLING_DATA_ERROR 
                                                    (METER_ID, METER_STREAM_NO, DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF,TIME_CREATE) 
                                                    VALUES (:meter_id, :meter_stream_no, TO_DATE(:data_date, 'DD-MM-YYYY'), :corrected_vol, :uncorrected_vol, :avr_pf, :avr_tf,:current_datetime)
                                                """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                        with connection.cursor() as cursor:
                                                            cursor.execute(sql_text_billing_insert, {
                                                                'meter_id': METERID,
                                                                'meter_stream_no': run,
                                                                'data_date': db_test_matched['DATA_DATE'].iloc[0],
                                                                'corrected_vol': corrected_vol,
                                                                'uncorrected_vol': uncorrected_vol,
                                                                'avr_pf': formatted_avr_pf,
                                                                'avr_tf': formatted_avr_tf,
                                                                'current_datetime':current_datetime_upper
                                                })
                                                            connection.commit()
                                                            print("Insert data ERROR successful")
                                                
                                            
                                            
                                            
                                            
                                            
                                                sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")
                                        else:
                                            print("No non-matching records found in the non-matching list.")
                                            sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")



                else:
                            for i in range(0, len(df_billing), 2):
                                db_test_matched = df_billing[i:i+2] 
                                        # print(db_test_matched)
                                db_test_matched['DATA_DATE'] = pd.to_datetime(db_test_matched['DATA_DATE']).dt.strftime('%d-%m-%Y')
                                if db_test_matched['DATA_DATE'].nunique() == 1 and db_test_matched['CORRECTED_VOL'].nunique() == 1 and db_test_matched['UNCORRECTED_VOL'].nunique() == 1:
                                                    print("Matched:", db_test_matched)
                                                    TIME_CREATE_MAX = f"""SELECT MAX(TIME_CREATE) AS MAX_TIME_CREATE FROM amr_billing_data WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'"""
                                                    MAX_TIME_CREATE = fetch_data(TIME_CREATE_MAX)
                                                    MAX_TIME_CREATE_ALL = MAX_TIME_CREATE[0][0]
                                                    sql_text_billing_matched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                                WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                                AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                                AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                                AND METER_ID = '{METERID}' 
                                                                                AND METER_STREAM_NO = '{run}'
                                                                                AND TIME_CREATE = '{MAX_TIME_CREATE_ALL}'"""
                                                    # print(sql_text_billing_matched_delete)
                                                    with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                        with connection.cursor() as cursor:
                                                            cursor.execute(sql_text_billing_matched_delete)
                                                            connection.commit()
                                                            print("Deleted matched data from billing successfully")  
                                                
                                                
                                                
                                else:
                                        print("Not Matched:", db_test_matched)
                                        query = f"""SELECT   DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data 
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                    AND meter_id = '{METERID}' 
                                                    AND meter_stream_no = '{run}' 
                                                    ORDER BY DATA_DATE DESC"""
                                        
                                        
                                        results_billing = fetch_data(query)
                                        sql_billing_error = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data_error
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY')
                                                    AND METER_ID = '{METERID}' 
                                                    AND METER_STREAM_NO = '{run}'"""
                                        billing_data_all_error = fetch_data(sql_billing_error)
                                        # Find matching records
                                        billing_data_all_dicts = [{'CORRECTED_VOL': row[1], 'UNCORRECTED_VOL': row[2], 'AVR_PF': float(row[3]), 'AVR_TF': float(row[4])} for row in results_billing]
                                        billing_data_all_error_dicts = [{'CORRECTED_VOL': int(row[0]), 'UNCORRECTED_VOL': int(row[1]), 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all_error]

                                        # Find matching records
                                        matching_records = []
                                        non_matching_records = []

                                        # Find matching and non-matching records
                                        for data in billing_data_all_dicts:
                                            match_found = False
                                            for error_data in billing_data_all_error_dicts:
                                                if (data['CORRECTED_VOL'] == error_data['CORRECTED_VOL'] and
                                                    data['UNCORRECTED_VOL'] == error_data['UNCORRECTED_VOL'] and
                                                    data['AVR_PF'] == error_data['AVR_PF'] and
                                                    data['AVR_TF'] == error_data['AVR_TF']):
                                                    matching_records.append(data)
                                                    match_found = True
                                                    break
                                            if not match_found:
                                                non_matching_records.append(data)

                                        # Process the matching records
                                        if matching_records:
                                            print("Matching Records:")
                                            for record in matching_records:
                                                print(record)
                                        else:
                                            print("No matching records found in the matching list.")

                                        # Process the non-matching records
                                        if non_matching_records:
                                            print("Non-matching Records:")
                                            for record in non_matching_records:
                                                # Extract values from the record
                                                print(record)
                                                corrected_vol = record['CORRECTED_VOL']
                                                uncorrected_vol = record['UNCORRECTED_VOL']
                                                
                                                formatted_avr_pf = f"{record['AVR_PF']:.5f}"
                                                formatted_avr_tf = f"{record['AVR_TF']:.2f}"
                                                sql_text_billing_insert = """
                                                    INSERT INTO AMR_BILLING_DATA_ERROR 
                                                    (METER_ID, METER_STREAM_NO, DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF,TIME_CREATE) 
                                                    VALUES (:meter_id, :meter_stream_no, TO_DATE(:data_date, 'DD-MM-YYYY'), :corrected_vol, :uncorrected_vol, :avr_pf, :avr_tf,:current_datetime)
                                                """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                        with connection.cursor() as cursor:
                                                            cursor.execute(sql_text_billing_insert, {
                                                                'meter_id': METERID,
                                                                'meter_stream_no': run,
                                                                'data_date': db_test_matched['DATA_DATE'].iloc[0],
                                                                'corrected_vol': corrected_vol,
                                                                'uncorrected_vol': uncorrected_vol,
                                                                'avr_pf': formatted_avr_pf,
                                                                'avr_tf': formatted_avr_tf,
                                                                'current_datetime':current_datetime_upper
                                                })
                                                            connection.commit()
                                                            print("Insert data ERROR successful")
                                                
                                            
                                            
                                            
                                            
                                            
                                                sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")
                                        else:
                                            print("No non-matching records found in the non-matching list.")
                                        
                                           
                                            sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")
            
            
            
        
        
        
        
        
        
        
        
        
        # ########### เช็คค่า poll ปกติ #####################
        else:
                    
            if values_subset == maxdate_billing_str:
                print(values_subset ,":",maxdate_billing_str)
                print("ปกติ")
                
                query = f"""SELECT  DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL 
                            FROM amr_billing_data 
                            WHERE DATA_DATE BETWEEN TO_DATE('{values_subset}', 'DD-MM-YYYY') - INTERVAL '{max_daily_value}' DAY AND TO_DATE('{values_subset}', 'DD-MM-YYYY') 
                            AND meter_id = '{METERID}' 
                            AND meter_stream_no = '{run}' 
                            ORDER BY DATA_DATE DESC"""
            
                
                results_billing = fetch_data(query)
                
                df_billing = pd.DataFrame(results_billing , columns=['DATA_DATE','CORRECTED_VOL', 'UNCORRECTED_VOL'])
                df_billing['DATA_DATE'] = df_billing['DATA_DATE'].dt.strftime('%d-%m-%Y')
                df_billing['DATA_DATE'] = pd.to_datetime(df_billing['DATA_DATE'], format='%d-%m-%Y')
                date_counts = df_billing['DATA_DATE'].value_counts()

                
                single_dates = date_counts[date_counts == 1].index.tolist()

                single_dates_formatted = pd.to_datetime(single_dates).strftime('%d-%m-%Y')
                single_date_rows = df_billing[df_billing['DATA_DATE'].isin(single_dates)]

            
                df_billing = df_billing[~df_billing['DATA_DATE'].isin(single_dates)]
                
                if len(single_dates_formatted) > 0:
                    # print(single_dates_formatted)
                    for date_formatted in single_dates_formatted:
                    
                        
                        print(date_formatted)
                        sql_text_billing_NotMatched = f"""SELECT DATA_DATE, METER_ID, METER_STREAM_NO 
                                                        FROM amr_billing_data_error
                                                        WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                                        AND METER_ID = '{METERID}' 
                                                        AND METER_STREAM_NO = '{run}'"""
                        billing_NotMatched = fetch_data(sql_text_billing_NotMatched)
                        
                       
                        sql_text_billing = f"""SELECT DATA_DATE, METER_ID, METER_STREAM_NO 
                                            FROM amr_billing_data
                                            WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                            AND METER_ID = '{METERID}' 
                                            AND METER_STREAM_NO = '{run}'"""
                        billing_data = fetch_data(sql_text_billing)
                        




                        if billing_NotMatched and billing_data:
                            print("ทั้งสองตัวแปรมีข้อมูล")

                            sql_billing = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                            FROM amr_billing_data
                                            WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                            AND METER_ID = '{METERID}' 
                                            AND METER_STREAM_NO = '{run}'"""
                            billing_data_all = fetch_data(sql_billing)
                            print("Billing Data All:", billing_data_all)

                            sql_billing_error = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data_error
                                                    WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY')
                                                    AND METER_ID = '{METERID}' 
                                                    AND METER_STREAM_NO = '{run}'"""
                            billing_data_all_error = fetch_data(sql_billing_error)
                            print("Billing Data All Error:", billing_data_all_error)

                            # Find matching records
                            billing_data_all_dicts = [{'CORRECTED_VOL': row[0], 'UNCORRECTED_VOL': row[1], 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all]
                            billing_data_all_error_dicts = [{'CORRECTED_VOL': int(row[0]), 'UNCORRECTED_VOL': int(row[1]), 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all_error]

                            # Find matching records
                            matching_records = []
                            for data in billing_data_all_dicts:
                                for error_data in billing_data_all_error_dicts:
                                    print(error_data)
                                    if (data['CORRECTED_VOL'] == error_data['CORRECTED_VOL'] and
                                        data['UNCORRECTED_VOL'] == error_data['UNCORRECTED_VOL'] and
                                        data['AVR_PF'] == error_data['AVR_PF'] and
                                        data['AVR_TF'] == error_data['AVR_TF']):
                                        matching_records.append(data)

                            if matching_records:
                                print("Matching Records:")
                                for record in matching_records:
                                    print(record)
                            else:
                                print("No matching records found.")
                                
                                sql_texts = []
                                for row in billing_data_all_dicts:
                                    formatted_avr_pf = f"{row['AVR_PF']:.5f}"
                                    formatted_avr_tf = f"{row['AVR_TF']:.2f}"

                                    sql_text_billing_insert = f"""
                                        INSERT INTO AMR_BILLING_DATA_ERROR 
                                        (METER_ID, METER_STREAM_NO, DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF,TIME_CREATE) 
                                        VALUES 
                                        ('{METERID}', '{run}', TO_DATE('{date_formatted}', 'DD-MM-YYYY'), 
                                        '{row['CORRECTED_VOL']}', '{row['UNCORRECTED_VOL']}', '{formatted_avr_pf}', '{formatted_avr_tf}','{current_datetime_upper}')
                                    """
                                    sql_texts.append(sql_text_billing_insert.strip())

                                full_sql_text = "\n".join(sql_texts)
                                print(full_sql_text)
                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(full_sql_text)
                                                        connection.commit()
                                                    print("Insert data ERROR successful")






                            sql_text_billing_NotMatched_delete = f"""DELETE FROM AMR_BILLING_DATA 
                                                                        WHERE DATA_DATE = TO_DATE('{date_formatted}', 'DD-MM-YYYY') 
                                                                        AND METER_ID = '{METERID}' 
                                                                        AND METER_STREAM_NO = '{run}'"""

                            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                    with connection.cursor() as cursor:
                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                        connection.commit()
                                        print("Deleted 'Not' Matched data from billing successfully")

                            
                                
                        else:
                            print("อย่างน้อยหนึ่งตัวแปรไม่มีข้อมูล")
                                
                                
                    for i in range(0, len(df_billing), 2):
                            db_test_matched = df_billing[i:i+2] 
                                    # print(db_test_matched)
                            db_test_matched['DATA_DATE'] = pd.to_datetime(db_test_matched['DATA_DATE']).dt.strftime('%d-%m-%Y')
                            if db_test_matched['DATA_DATE'].nunique() == 1 and db_test_matched['CORRECTED_VOL'].nunique() == 1 and db_test_matched['UNCORRECTED_VOL'].nunique() == 1:
                                                print("Matched:", db_test_matched)
                                                TIME_CREATE_MAX = f"""SELECT MAX(TIME_CREATE) AS MAX_TIME_CREATE FROM amr_billing_data WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'"""
                                                MAX_TIME_CREATE = fetch_data(TIME_CREATE_MAX)
                                                MAX_TIME_CREATE_ALL = MAX_TIME_CREATE[0][0]
                                                print(MAX_TIME_CREATE_ALL)
                                                sql_text_billing_matched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            
                                                                            AND TIME_CREATE = '{MAX_TIME_CREATE_ALL}'"""
                                                # print(sql_text_billing_matched_delete)
                                                # print(sql_text_billing_matched_delete)
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_matched_delete)
                                                        connection.commit()
                                                        print("Deleted matched data from billing successfully")  
                                            
                                            
                                            
                            else:
                                        print("Not Matched:", db_test_matched)
                                        query = f"""SELECT   DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data 
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                    AND meter_id = '{METERID}' 
                                                    AND meter_stream_no = '{run}' 
                                                    ORDER BY DATA_DATE DESC"""
                                        
                                        
                                        results_billing = fetch_data(query)
                                        sql_billing_error = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data_error
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY')
                                                    AND METER_ID = '{METERID}' 
                                                    AND METER_STREAM_NO = '{run}'"""
                                        billing_data_all_error = fetch_data(sql_billing_error)
                                        # Find matching records
                                        billing_data_all_dicts = [{'CORRECTED_VOL': row[1], 'UNCORRECTED_VOL': row[2], 'AVR_PF': float(row[3]), 'AVR_TF': float(row[4])} for row in results_billing]
                                        billing_data_all_error_dicts = [{'CORRECTED_VOL': int(row[0]), 'UNCORRECTED_VOL': int(row[1]), 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all_error]

                                        # Find matching records
                                        matching_records = []
                                        non_matching_records = []

                                        # Find matching and non-matching records
                                        for data in billing_data_all_dicts:
                                            match_found = False
                                            for error_data in billing_data_all_error_dicts:
                                                if (data['CORRECTED_VOL'] == error_data['CORRECTED_VOL'] and
                                                    data['UNCORRECTED_VOL'] == error_data['UNCORRECTED_VOL'] and
                                                    data['AVR_PF'] == error_data['AVR_PF'] and
                                                    data['AVR_TF'] == error_data['AVR_TF']):
                                                    matching_records.append(data)
                                                    match_found = True
                                                    break
                                            if not match_found:
                                                non_matching_records.append(data)

                                        # Process the matching records
                                        if matching_records:
                                            print("Matching Records:")
                                            for record in matching_records:
                                                print(record)
                                        else:
                                            print("No matching records found in the matching list.")

                                        # Process the non-matching records
                                        if non_matching_records:
                                            print("Non-matching Records:")
                                            for record in non_matching_records:
                                                # Extract values from the record
                                                print(record)
                                                corrected_vol = record['CORRECTED_VOL']
                                                uncorrected_vol = record['UNCORRECTED_VOL']
                                                
                                                formatted_avr_pf = f"{record['AVR_PF']:.5f}"
                                                formatted_avr_tf = f"{record['AVR_TF']:.2f}"
                                                sql_text_billing_insert = """
                                                    INSERT INTO AMR_BILLING_DATA_ERROR 
                                                    (METER_ID, METER_STREAM_NO, DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF,TIME_CREATE) 
                                                    VALUES (:meter_id, :meter_stream_no, TO_DATE(:data_date, 'DD-MM-YYYY'), :corrected_vol, :uncorrected_vol, :avr_pf, :avr_tf,:current_datetime)
                                                """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                        with connection.cursor() as cursor:
                                                            cursor.execute(sql_text_billing_insert, {
                                                                'meter_id': METERID,
                                                                'meter_stream_no': run,
                                                                'data_date': db_test_matched['DATA_DATE'].iloc[0],
                                                                'corrected_vol': corrected_vol,
                                                                'uncorrected_vol': uncorrected_vol,
                                                                'avr_pf': formatted_avr_pf,
                                                                'avr_tf': formatted_avr_tf,
                                                                'current_datetime':current_datetime_upper
                                                })
                                                            connection.commit()
                                                            print("Insert data ERROR successful")
                                                
                                            
                                            
                                            
                                            
                                            
                                                sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")
                                        else:
                                            print("No non-matching records found in the non-matching list.")
                                            sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")
                                    
            else:
                        
                    for i in range(0, len(df_billing), 2):
                            db_test_matched = df_billing[i:i+2] 
                                    # print(db_test_matched)
                            db_test_matched['DATA_DATE'] = pd.to_datetime(db_test_matched['DATA_DATE']).dt.strftime('%d-%m-%Y')
                            if db_test_matched['DATA_DATE'].nunique() == 1 and db_test_matched['CORRECTED_VOL'].nunique() == 1 and db_test_matched['UNCORRECTED_VOL'].nunique() == 1:
                                                print("Matched:", db_test_matched)
                                                TIME_CREATE_MAX = f"""SELECT MAX(TIME_CREATE) AS MAX_TIME_CREATE FROM amr_billing_data WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'"""
                                                MAX_TIME_CREATE = fetch_data(TIME_CREATE_MAX)
                                                MAX_TIME_CREATE_ALL = MAX_TIME_CREATE[0][0]
                                                
                                                sql_text_billing_matched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND CORRECTED_VOL = {db_test_matched['CORRECTED_VOL'].iloc[0]} 
                                                                            AND UNCORRECTED_VOL = {db_test_matched['UNCORRECTED_VOL'].iloc[0]} 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            AND TIME_CREATE = '{MAX_TIME_CREATE_ALL}'"""
                                                # print(sql_text_billing_matched_delete)
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_matched_delete)
                                                        connection.commit()
                                                        print("Deleted matched data from billing successfully")  
                                            
                                            
                                            
                            else:
                                        print("Not Matched:", db_test_matched)
                                        query = f"""SELECT   DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data 
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                    AND meter_id = '{METERID}' 
                                                    AND meter_stream_no = '{run}' 
                                                    ORDER BY DATA_DATE DESC"""
                                        
                                        
                                        results_billing = fetch_data(query)
                                        sql_billing_error = f"""SELECT CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF 
                                                    FROM amr_billing_data_error
                                                    WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY')
                                                    AND METER_ID = '{METERID}' 
                                                    AND METER_STREAM_NO = '{run}'"""
                                        billing_data_all_error = fetch_data(sql_billing_error)
                                        # Find matching records
                                        billing_data_all_dicts = [{'CORRECTED_VOL': row[1], 'UNCORRECTED_VOL': row[2], 'AVR_PF': float(row[3]), 'AVR_TF': float(row[4])} for row in results_billing]
                                        billing_data_all_error_dicts = [{'CORRECTED_VOL': int(row[0]), 'UNCORRECTED_VOL': int(row[1]), 'AVR_PF': float(row[2]), 'AVR_TF': float(row[3])} for row in billing_data_all_error]

                                        # Find matching records
                                        matching_records = []
                                        non_matching_records = []

                                        # Find matching and non-matching records
                                        for data in billing_data_all_dicts:
                                            match_found = False
                                            for error_data in billing_data_all_error_dicts:
                                                if (data['CORRECTED_VOL'] == error_data['CORRECTED_VOL'] and
                                                    data['UNCORRECTED_VOL'] == error_data['UNCORRECTED_VOL'] and
                                                    data['AVR_PF'] == error_data['AVR_PF'] and
                                                    data['AVR_TF'] == error_data['AVR_TF']):
                                                    matching_records.append(data)
                                                    match_found = True
                                                    break
                                            if not match_found:
                                                non_matching_records.append(data)

                                        # Process the matching records
                                        if matching_records:
                                            print("Matching Records:")
                                            for record in matching_records:
                                                print(record)
                                        else:
                                            print("No matching records found in the matching list.")

                                        # Process the non-matching records
                                        if non_matching_records:
                                            print("Non-matching Records:")
                                            for record in non_matching_records:
                                                # Extract values from the record
                                                print(record)
                                                corrected_vol = record['CORRECTED_VOL']
                                                uncorrected_vol = record['UNCORRECTED_VOL']
                                                
                                                formatted_avr_pf = f"{record['AVR_PF']:.5f}"
                                                formatted_avr_tf = f"{record['AVR_TF']:.2f}"
                                                sql_text_billing_insert = """
                                                    INSERT INTO AMR_BILLING_DATA_ERROR 
                                                    (METER_ID, METER_STREAM_NO, DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF,TIME_CREATE) 
                                                    VALUES (:meter_id, :meter_stream_no, TO_DATE(:data_date, 'DD-MM-YYYY'), :corrected_vol, :uncorrected_vol, :avr_pf, :avr_tf,:current_datetime)
                                                """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                        with connection.cursor() as cursor:
                                                            cursor.execute(sql_text_billing_insert, {
                                                                'meter_id': METERID,
                                                                'meter_stream_no': run,
                                                                'data_date': db_test_matched['DATA_DATE'].iloc[0],
                                                                'corrected_vol': corrected_vol,
                                                                'uncorrected_vol': uncorrected_vol,
                                                                'avr_pf': formatted_avr_pf,
                                                                'avr_tf': formatted_avr_tf,
                                                                'current_datetime':current_datetime_upper
                                                })
                                                            connection.commit()
                                                            print("Insert data ERROR successful")
                                                
                                            
                                            
                                            
                                            
                                            
                                                sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")
                                        else:
                                            print("No non-matching records found in the non-matching list.")
                                        
                                           
                                            sql_text_billing_NotMatched_delete= f"""DELETE FROM AMR_BILLING_DATA 
                                                                            WHERE DATA_DATE = TO_DATE('{db_test_matched['DATA_DATE'].iloc[0]}', 'DD-MM-YYYY') 
                                                                            AND METER_ID = '{METERID}' 
                                                                            AND METER_STREAM_NO = '{run}'
                                                                            """
                                            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                                    with connection.cursor() as cursor:
                                                        cursor.execute(sql_text_billing_NotMatched_delete)
                                                        connection.commit()
                                                        print("Deleted 'Not' Matched data from billing successfully")








        sql_text_config_delete = f"""delete from AMR_CONFIGURED_DATA where METER_ID = '{METERID}' AND METER_STREAM_NO = '{run}' AND DATA_DATE = TO_DATE('{date_system}', 'DD-MM-YYYY')"""
        
        sql_text_config_insert = "insert into AMR_CONFIGURED_DATA (DATA_DATE, METER_ID,METER_STREAM_NO, AMR_VC_TYPE,TIME_CREATE, "
        for i in range(0, len(df_mapping)):  
                
            sql_text_config_insert+=f" AMR_CONFIG{i+1},"
        sql_text_config_insert+=" CREATED_BY) values ("
            
        sql_text_config_insert+=f"TO_DATE('{date_system}', 'DD-MM-YYYY'), '{METERID}','{run}','{evc_type}','{current_datetime_upper}',"
        
            
        for i in range(0, len(df_mapping)):
            
                sql_text_config_insert+=f"'{str(list_of_values_configured[i])}',"
                
        sql_text_config_insert+="'')" 
        # print(sql_text_config_insert)

        get_maxdate =f"SELECT MAX(DATA_DATE) FROM amr_configured_data WHERE meter_id = '{METERID}' AND meter_stream_no = '{run}'"
        
        cursor = fetch_data(get_maxdate)
        config_db = pd.DataFrame(cursor,columns=['DATA_DATE'])
            
        config_db['DATA_DATE'] = pd.to_datetime(config_db['DATA_DATE'])

            # Access the first row and format the date
        config_db_1 = config_db.iloc[0]['DATA_DATE'].strftime('%d-%m-%Y')
        # print(date_system , ":",config_db_1)
        if date_system == config_db_1:
                print("config มีข้อมูลของวันนี้เเล้ว")
                
                config_delete = f"DELETE FROM AMR_CONFIGURED_DATA WHERE DATA_DATE = TO_DATE('{date_system}', 'DD-MM-YYYY') AND METER_ID = '{METERID}' AND METER_STREAM_NO = '{run}' AND AMR_VC_TYPE = '{evc_type}'"
                
                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(config_delete)  
                    connection.commit()  
                    print("Insert data 'config_delete' successful")
                    
                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql_text_config_insert)  
                    connection.commit()  
                    print("Insert data 'config' successful")
                
            

        else:
            
                with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql_text_config_insert)  
                    connection.commit()  
                    print("Insert data 'config' successful")

                    
        successful_rows += 1
        print(f"ชุดที่ {completed_sets} เสร็จสิ้นแล้ว\n")
        successful_message_test.append(METERID) 
    except Exception as e:
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_time_error = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        
        tcp_ip = row[6]
        tcp_port = row[7]
        run = row[3]
        Sitename = row[1]
        METERID = row[4]
        VCtype = row[12]
        GRANT_UPDATE = """GRANT UPDATE ON AMR_FIELD_METER TO PTT_PIVOT"""
        update_pm = f"""UPDATE AMR_FIELD_METER SET METER_POLL_REPEAT1 = '1' WHERE METER_ID = '{METERID}' AND METER_STREAM_NO = '{run}'"""
       
        with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                                        with connection.cursor() as cursor:
                
        
                                            cursor.execute(GRANT_UPDATE)
                                            connection.commit()
                                            cursor.execute(update_pm)
                                            connection.commit()
                                           
                                            print("update METER_POLL_REPEAT1 successfully")
        traceback_info = traceback.format_exc()  # ข้อมูลเกี่ยวกับ traceback
        error_message = f"Error occurred at {current_time} - TCP IP: {tcp_ip}, Port: {tcp_port}, Run: {run}, METERID: {METERID}. Error: {type(e).__name__}: {str(e)}"
        logger_error.error(error_message)
        


        traceback.print_exc()
        
         
        error_desc = f"{str(e)}"
        max_length = 200

        if len(error_desc) > max_length:
            error_desc = error_desc[:max_length]

        error_desc = error_desc.replace("'", "''")

        sql_text_billing_insert = f"""
            INSERT INTO AMR_ERROR (
                METER_ID, METER_STREAM_NO, DATA_DATE, TAG_ID, ERROR_DESC, EVC_TYPE
            ) VALUES (
                '{METERID}', '{run}', TO_DATE('{current_time_error}', 'DD-MM-YYYY HH24:MI:SS'), '{Sitename}', '{error_desc}', '{VCtype}'
            )
        """

        with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text_billing_insert)
                connection.commit()
                print("INSERT AMR_ERROR successfully")



        error_message_test.append(METERID) 
        
        print("Skipping to the next row.")
        error_count += 1
        continue

if successful_message_test:
    print("All successful Messages:")
    for msg in successful_message_test:
        print(msg)

# Print all error messages if they exist
if error_message_test:
    print("All Error Messages:")
    for msg in error_message_test:
        print(msg)

# Count occurrences of unique successful METERIDs and print them
counter_1 = Counter(successful_message_test)
if counter_1:
    print(f"Total unique successful METERIDs: {len(counter_1.keys())}")
    print("All successful METERIDs (unique):")
    for meter_id in counter_1.keys():
        print(meter_id)

# Count occurrences of unique error METERIDs and print them
counter_1 = Counter(successful_message_test)
if counter_1:
    print(f"Total unique successful METERIDs: {len(counter_1.keys())}")
    print("All successful METERIDs (unique):")
    for meter_id in counter_1.keys():
        print(meter_id)

# Count occurrences of unique error METERIDs and print them

counter = Counter(error_message_test)
if counter:
    print(f"Total unique Error METERIDs: {len(counter.keys())}")
    print("All Error METERIDs (unique):")
    for meter_id in counter.keys():
        print(meter_id)
        
    







# Find duplicate METERIDs in both lists

counter = Counter(error_message_test)
if counter:
    print(f"Total unique Error METERIDs: {len(counter.keys())}")
    print("All Error METERIDs (unique):")
    for meter_id in counter.keys():
        print(meter_id)

    

duplicate_meter_ids = set(counter.keys()) & set(counter_1.keys())
if duplicate_meter_ids:
    print("Duplicate METERIDs found in both error and successful lists:")
    for meter_id in duplicate_meter_ids:
        print(meter_id)

    # Remove duplicates from counter_1
    for meter_id in duplicate_meter_ids:
        del counter_1[meter_id]

    # Recount and print unique successful METERIDs after removing duplicates
    if counter_1:
        print(f"Total unique successful METERIDs after removing duplicates: {len(counter_1.keys())}")
        print("All successful METERIDs (unique) after removing duplicates:")
        for meter_id in counter_1.keys():
            print(meter_id)
            
        
else:
    print("No duplicate METERIDs found.")






total_site = len(counter_1.keys()) + len(counter.keys())
print("total_site",total_site)
total_sets = successful_rows + error_count
logger_info = logging.getLogger('info_logger')
logger_info.setLevel(logging.INFO)


log_file_info = 'C:\\Users\\Administrator\\Desktop\\autopoll\\Autopoll_repeat1\\total.log'
file_handler_info = logging.FileHandler(log_file_info)
formatter_info = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
file_handler_info.setFormatter(formatter_info)
logger_info.addHandler(file_handler_info)

logger_info.info(f"Actaris(U) : Total sets processed: {total_sets}, Successful rows: {successful_rows}, Errors: {error_count}") 




logger_info_site = logging.getLogger('info_logger_site')
logger_info_site.setLevel(logging.INFO)
log_file_info_site = 'C:\\Users\\Administrator\\Desktop\\autopoll\\Autopoll_repeat1\\total_site.log'
file_handler_info_site = logging.FileHandler(log_file_info_site)
formatter_info_site = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
file_handler_info_site.setFormatter(formatter_info_site)
logger_info_site.addHandler(file_handler_info_site)

logger_info_site.info(f"Actaris(U) : Total site processed: {total_site}, Successful rows: {len(counter_1.keys())}, Errors: {len(counter.keys())}") 

if __name__ == '__main__':
    
    exit()