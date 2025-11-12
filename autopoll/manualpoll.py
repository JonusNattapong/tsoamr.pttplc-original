## Parse argument --repeat xx and --type-d xx  

import argparse
import pandas as pd
import cx_Oracle
from flask import flash
import traceback
from datetime import datetime, timedelta
from flask import abort
import socket
#from pymodbus.utilities import computeCRC
import time
import datetime
import logging
from collections import Counter
#### Tul
from convert_modbus import convert_raw_to_value
import math

import os
import sys
from pathlib import Path
# เพิ่ม root directory (my_project) ลงใน sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

#import computeCRC
import modbusdrv
import dotenv
from dotenv import load_dotenv

communication_traffic = []
change_to_32bit_counter = 0 

def verifyNumericReturnNULL(value):
    if (isinstance(value, (int, float)) and math.isnan(value)) or value == '' or value == 'None' or value == 'NONE' or value is None:
        return 'NULL' 
    return value

def is_valid_date(date_str, date_format='%d-%m-%Y'):
    # ตรวจสอบว่าค่า string ทั้งหมดเป็น "0" หรือไม่
    if all(char == '0' for char in date_str):
        return False

    try:
        # พยายามแปลงค่าที่ได้รับเป็น datetime object ตามรูปแบบที่กำหนด
        datetime.datetime.strptime(date_str, date_format)
        return True  # ถ้าแปลงได้สำเร็จ แสดงว่าเป็นค่าที่ถูกต้อง
    except ValueError:
        return False  # ถ้าเกิด ValueError แสดงว่าไม่ใช่ค่าที่ถูกต้อง 

def convert_to_binary_string(value, bytes_per_value):
    binary_string = bin(value)[
        2:
    ]  
    return binary_string.zfill(
        bytes_per_value * 8
    )  

def convert_raw_to_value_config(data_type, raw_data):
    data = convert_raw_to_value(data_type, raw_data, "config")   
    return data 

def create_SQL_text_insert_Billing(meterid, run, current_datetime_upper,  data_date, corrected, uncorrected, avr_pf, avr_tf):
    if avr_pf == 'NULL':
        avr_pf = "NULL"
    if avr_tf == 'NULL':
        avr_tf = "NULL"
    sql_text_billing_insert = (
        f"INSERT INTO AMR_BILLING_DATA (METER_ID, METER_STREAM_NO, DATA_DATE,TIME_CREATE, "
        f"CORRECTED_VOL ,UNCORRECTED_VOL, AVR_PF, AVR_TF) VALUES ('{meterid}', '{run}', "
        f"TO_DATE('{data_date}', 'DD-MM-YYYY'),")

    sql_text_billing_insert += f"'{current_datetime_upper}'"
    sql_text_billing_insert += f", {corrected}"
    sql_text_billing_insert += f", {uncorrected}"
    sql_text_billing_insert += f", {avr_pf}"
    sql_text_billing_insert += f", {avr_tf}"
    sql_text_billing_insert += ");"

    return sql_text_billing_insert

def create_SQL_text_insert_Billing_error(meterid, run, current_datetime_upper,  data_date, corrected, uncorrected, avr_pf, avr_tf):
    if avr_pf == 'NULL':
        avr_pf = "NULL"
    if avr_tf == 'NULL':
        avr_tf = "NULL"
    sql_text_billing_insert = (
        f"INSERT INTO AMR_BILLING_DATA_ERROR (METER_ID, METER_STREAM_NO, DATA_DATE,TIME_CREATE, "
        f"CORRECTED_VOL ,UNCORRECTED_VOL, AVR_PF, AVR_TF) SELECT '{meterid}', '{run}', "
        f"TO_DATE('{data_date}', 'DD-MM-YYYY'),")

    sql_text_billing_insert += f"'{current_datetime_upper}'"
    sql_text_billing_insert += f", {corrected}"
    sql_text_billing_insert += f", {uncorrected}"
    sql_text_billing_insert += f", {avr_pf}"
    sql_text_billing_insert += f", {avr_tf}"
    
    sql_text_billing_insert += f" FROM DUAL WHERE NOT EXISTS ( SELECT 1 FROM AMR_BILLING_DATA_ERROR WHERE "
    sql_text_billing_insert += f"METER_ID = '{meterid}'"
    sql_text_billing_insert += f" AND METER_STREAM_NO = '{run}'"
    sql_text_billing_insert += f" AND DATA_DATE = TO_DATE('{data_date}', 'DD-MM-YYYY')"
    sql_text_billing_insert += f" AND CORRECTED_VOL = {corrected}"
    sql_text_billing_insert += f" AND UNCORRECTED_VOL = {uncorrected}"
    sql_text_billing_insert += f" AND AVR_PF = {avr_pf}"
    sql_text_billing_insert += f" AND AVR_TF = {avr_tf}"
    sql_text_billing_insert += ");"
    return sql_text_billing_insert

def create_SQL_text_delete_Billing(meterid, run, data_date):
    sql_text_billing_delete = (f"DELETE FROM AMR_BILLING_DATA WHERE DATA_DATE = TO_DATE('{data_date}', 'DD-MM-YYYY') AND METER_ID = '{meterid}' AND METER_STREAM_NO = '{run}'""")
    sql_text_billing_delete += ";"
    return sql_text_billing_delete

# โหลดค่าในไฟล์ .env
load_dotenv()

username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
hostname = os.getenv("DB_HOSTNAME")
port = os.getenv("DB_PORT")
service_name = os.getenv("DB_SERVICE")


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

def reset_all_repeat():
    pass
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Select rount repeat default =1 ")

    parser.add_argument("--sitename", type=str, default=1, help="Input  --Site name")
    parser.add_argument("--runno", type=int, default=5, help="Input --run no 1,2,3,4.... ")
    
    args = parser.parse_args()
    sitename_poll = args.sitename
    runno = args.runno
                                                    
                                            
    logger_error = logging.getLogger('error_logger')
    logger_error.setLevel(logging.ERROR)

    query = f"""SELECT
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
                        AMR_VC_TYPE.id as evctype
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
                        AMR_FIELD_ID.FIELD_ID = AMR_PL_GROUP.FIELD_ID AND
                        AMR_FIELD_ID.METER_ID = AMR_USER.USER_GROUP AND
                        AMR_FIELD_ID.CUST_ID = AMR_FIELD_CUSTOMER.CUST_ID AND
                        AMR_FIELD_ID.METER_ID = AMR_FIELD_METER.METER_ID AND
                        AMR_VC_TYPE.ID = AMR_FIELD_METER.METER_STREAM_TYPE AND
                        AMR_FIELD_METER.METER_PORT_NO = AMR_PORT_INFO.ID AND
                        amr_poll_range.evc_type = AMR_VC_TYPE.id AND
                        AMR_FIELD_ID.TAG_ID like '{sitename_poll}' AND AMR_FIELD_METER.METER_STREAM_NO = '{runno}'   
    """

    rows =  fetch_data(query)

    successful_message_test= []
    error_message_test = []
    
    for row in rows:
        try:
            Sitename = row[1]
            
            VCtype = row[12]
            tcp_ip = row[6]
            tcp_port = row[7]
            run = row[3]
            METERID = row[4]
            poll_config_set = row[8]
            poll_billing_set = row[9]
            CONFIG_ENABLE_set = row[10].replace(',', '')
            BILLING_ENABLE_set = row[11].replace(',', '')
            
            print(tcp_ip)
            print(tcp_port)
            evc_type = row[12]
            print(evc_type)
            print(METERID)
            slave_id = 1
            function_code = 3
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

            for i in range(1, len(CONFIG_ENABLE_set)+1):
                if CONFIG_ENABLE_set[i-1] == '1':
                    starting_address_i = starting_address_config_[i]
                    quantity_i = quantity_config_[i]
                    adjusted_quantity_i = quantity_i - starting_address_i + 1
                    data = {'starting_address_i': [starting_address_i], 
                            'quantity_i': [quantity_i], 
                            'adjusted_quantity_i': [adjusted_quantity_i]}
                    df_2 = pd.DataFrame(data)
                    
                    df_pollRange = pd.concat([df_pollRange,df_2] , ignore_index=True)

            for i in range(1, len(BILLING_ENABLE_set)+1): 
                
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

            if int(evc_type) in [5, 8, 9, 10, 12, 17]:
            # #send wa
                for _ in range(2):
                    modbusdrv.wakeupEVC(sock_i)
                    time.sleep(1)

            for i in range(0, len(df_pollRange)):
                start_address = int(df_pollRange.loc[i,'starting_address_i'])   
                adjusted_quantity = int(df_pollRange.loc[i,'adjusted_quantity_i'])
                
                request_message_i = modbusdrv.modbus_package(slave_id, function_code, start_address, adjusted_quantity )
                communication_traffic_i = []            
                communication_traffic_i.append(request_message_i.hex())
                print(f"Poll Config:{i}, start:{start_address}, length:{adjusted_quantity}")
                response_i = modbusdrv.tx_socket(sock_i, request_message_i, 1.8, 1)
                communication_traffic_i.append(response_i.hex())

                if response_i[1:2] != b'\x03':
                        abort(400, f"Error: Unexpected response code from device {communication_traffic_i[1]}!")
                else:
                        pass
            
                data = {
                    'address_start': [int(start_address)],
                    'finish': [int(start_address+adjusted_quantity)],
                    'TX': [communication_traffic_i[0]],
                    'RX': [communication_traffic_i[1]]
                }
                
                df_2 = pd.DataFrame(data)
                df_Modbus = pd.concat([df_Modbus, df_2], ignore_index=True)
            
            for i in range(0, len(df_pollBilling)):
                start_address = int(df_pollBilling.loc[i,'starting_address_i'])
                adjusted_quantity = int(df_pollBilling.loc[i,'adjusted_quantity_i'])
                
                request_message_i = modbusdrv.modbus_package(slave_id, function_code, start_address, adjusted_quantity )
                communication_traffic_i = []  
                communication_traffic_i.append(request_message_i.hex())

                print(f"Poll Billing:{i}, start:{start_address}, length:{adjusted_quantity}")
                response_i = modbusdrv.tx_socket(sock_i, request_message_i, 1.8, 1)   
                communication_traffic_i.append(response_i.hex())

                if response_i[1:2] != b'\x03':
                        abort(400, f"Error: Unexpected response code from device {communication_traffic_i[1]}!")
                else:
                        pass

                data = {
                    'address_start': [int(start_address)],
                    'finish': [int(start_address+adjusted_quantity-1)],
                    'TX': [communication_traffic_i[0]],
                    'RX': [communication_traffic_i[1]]
                }
                # print(data)
                df_2 = pd.DataFrame(data)
                df_Modbusbilling = pd.concat([df_Modbusbilling, df_2], ignore_index=True)
                
            
            ##############################################################
            ### poll success
            # keep Modbus in df_Modbus for Configdata and df_Monbusbilling for Billing data
            
            query = f"select amc.or_der as order1, amc.address as address1, amc.description as desc1, amc.data_type as dtype1 \
            from amr_mapping_config amc \
            where amc.evc_type = '{evc_type}' AND address is not null \
            order by order1"
            
            cursor = fetch_data(query)
            df_mapping = pd.DataFrame(cursor, columns=['order', 'address', 'desc', 'data_type'])
            #print(df_mapping)

            list_of_values_configured = []
            for i in range(0, len(df_mapping)):
                
                address = int(df_mapping.iloc[i,1])
                
                data_type = str(df_mapping.iloc[i,3])
                            
                for j in range(0,len(df_Modbus)):
                    address_start = int(df_Modbus.iloc[j,0])
                    address_finish = int(df_Modbus.iloc[j,1])
                    
                    if address >= address_start and address <= address_finish:
                        # print(address_start, address_finish, df_Modbus.iloc[j,3])
                        location_data = (address - address_start)*int(8/2)
                        frameRx = (df_Modbus.iloc[j,3])
                        #
                        if data_type == "EVODate":
                            raw_data = frameRx[location_data + 6: location_data + 18]
                        else :
                            raw_data = frameRx[location_data + 6: location_data + 14]
                        
                        list_of_values_configured.append(convert_raw_to_value_config(data_type, raw_data))
                        # 
                        break
                    elif address == 0:
                        list_of_values_configured.append('0')
                        break
            
            # Billing Data
            query = f"SELECT amb.daily ,amb.or_der ,amb.address,amb.description,amb.data_type  FROM amr_mapping_billing amb WHERE amb.evc_type = '{evc_type}' AND address is not null order by amb.daily,amb.or_der"
            cursor = fetch_data(query)
            
            df_mappingbilling = pd.DataFrame(cursor, columns=['daily','or_der', 'address', 'description', 'data_type'])
            print(df_mappingbilling)
            
            
            #### BILLING
            list_of_values_billing = []
            check_first_date = 0
            for i in range(len(df_mappingbilling)):
                address = int(df_mappingbilling.iloc[i,2])                
                data_type = str(df_mappingbilling.iloc[i,4])
                
                for j in range(0,len(df_Modbusbilling)):
                    address_start = int(df_Modbusbilling.iloc[j,0])
                    address_finish = int(df_Modbusbilling.iloc[j,1])

                    if address >= address_start and address <= address_finish:
                    
                        location_data = (address - address_start)*int(8/2)
                        frameRx = (df_Modbusbilling.iloc[j,3])
                        
                        #HARD CODE
                        if data_type == "EVODate":
                            raw_data = frameRx[location_data + 6: location_data + 18]
                        else :
                            raw_data = frameRx[location_data + 6: location_data + 14]
                        
                        # Verify first day data from Date
                        data_calc = convert_raw_to_value(data_type,raw_data, "billing")
                        if check_first_date == 0: 
                            if data_type in ["Date", "EVODate"]:
                                check_first_date = 1
                                firstday_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%d-%m-%Y')
                                # Verify the first day
                                if data_calc != firstday_str:
                                    raise Exception("First date invalid")
                        
                        #    list_of_values_billing.append(convert_raw_to_value(data_type,raw_data, "billing"))
                        list_of_values_billing.append(data_calc)
                        break
            ## have list of Config and List fo Billing
            ## Validate billing data


            list_cut = []
            day_polled = 0
            max_day_polled = df_mappingbilling['daily'].max()
            max_order = df_mappingbilling['or_der'].max()
            # for i in range(0, len(df_mappingbilling),  max_order):
            #     #values_subset = list_of_values_billing[i:i+(max_order-1)]

            #     ## debug no check date
            #     # if not is_valid_date(values_subset[0]):
            #     #     continue
            #     ## Debug
            #     #date_str = str(values_subset[0]).zfill(8)  # แปลงเป็น string ก่อน
            #     #formatted_date = f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:]}"

            #     #list_cut.append(f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:]}")
            #     list_cut.extend(list_of_values_billing[i:i + max_order])
            #     day_polled += 1
                    
            # list_of_values_billing = list_cut.copy()
            #### BILLING

            # print data 
            print(f"{METERID},Site : {Sitename}, run {runno}")
            # Config
            for i in range(0, len(df_mapping)):
                print(f"{df_mapping.iloc[i,2]} : {list_of_values_configured[i]}")

            # Billing
            print(f"   DATE | Corrected | Uncorrected | avr_Pf  | avr_Tf ")
            for i in range(0, max_day_polled):
                    ### Billing 5 data
                    # values_subset = list_of_values_billing[(i*5):(i*5)+5] 
                    # date_polled = values_subset[0]
                    # corrected_polled = verifyNumericReturnNULL(values_subset[1])
                    # uncorrected_polled = verifyNumericReturnNULL(values_subset[2])
                    # avr_pf_polled = verifyNumericReturnNULL(values_subset[3])
                    # avr_tf_polled = verifyNumericReturnNULL(values_subset[4])
                    # print(f"{date_polled} | {corrected_polled} | {uncorrected_polled} | {avr_pf_polled} | {avr_tf_polled}")
                    ## Billing 5 Data
                    
                    for j in range (max_order):
                        jj = (i*max_order)+j
                        txt_print = f"day{i+1}, Billing{j+1} : {df_mappingbilling.iloc[j,3]} : {list_of_values_billing[(i*max_order)+j]}"
                        print(txt_print)
                    print("--------------")




            # save_db = input("Save to Database[Y/N]")
            save_db = 'n'
            if  save_db == 'Y' or save_db =='y':
                current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime('%d-%b-%y %I.%M.%S.%f %p ASIA/BANGKOK')
                current_datetime_upper = current_datetime.upper()
                date_system = datetime.datetime.now().strftime('%d-%m-%Y')   
                
                full_sql_text = ""
                #for i in range(0, len(df_mappingbilling), 5):    
                #        values_subset = list_of_values_billing[i:i+5]
                for i in range(0, day_polled):
                    values_subset = list_of_values_billing[(i*5):(i*5)+5]
                    date_polled = values_subset[0]
                    corrected_polled = verifyNumericReturnNULL(values_subset[1])
                    uncorrected_polled = verifyNumericReturnNULL(values_subset[2])
                    avr_pf_polled = verifyNumericReturnNULL(values_subset[3])
                    avr_tf_polled = verifyNumericReturnNULL(values_subset[4])

                    # query for checked 
                    sql_billing_DB = f"""SELECT DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF, METER_ID, METER_STREAM_NO 
                                                                FROM amr_billing_data
                                                                WHERE DATA_DATE = TO_DATE('{date_polled}', 'DD-MM-YYYY')
                                                                AND METER_ID = '{METERID}' 
                                                                AND METER_STREAM_NO = '{run}'"""
                    
                    billing_DB = fetch_data(sql_billing_DB)
                    
                    if billing_DB:
                        # already have data check
                        date_db = billing_DB[0][0]
                        corrected_db = verifyNumericReturnNULL(billing_DB[0][1])
                        uncorrected_db = verifyNumericReturnNULL(billing_DB[0][2])
                        avr_pf_db = verifyNumericReturnNULL(billing_DB[0][3])
                        avr_tf_db = verifyNumericReturnNULL(billing_DB[0][4])
                        

                        if (corrected_polled == corrected_db and 
                            uncorrected_polled == uncorrected_db and 
                            (avr_pf_polled == avr_pf_db or (avr_pf_polled is None and avr_pf_db is None)) and 
                            (avr_tf_polled == avr_tf_db or (avr_tf_polled is None and avr_tf_db is None))):

                            # case 0 have billing match = do nothing
                            pass
                        else :
                            # case 4 not match  =  delete from billing -> insert both into error
                            full_sql_text = full_sql_text + create_SQL_text_delete_Billing(METERID, run, date_polled) + "\n"
                            full_sql_text = full_sql_text + create_SQL_text_insert_Billing_error(METERID, run, current_datetime_upper, date_polled,corrected_polled, uncorrected_polled, avr_pf_polled, avr_tf_polled) + "\n"
                            full_sql_text = full_sql_text + create_SQL_text_insert_Billing_error(METERID, run, current_datetime_upper, date_polled,corrected_db, uncorrected_db, avr_pf_db, avr_tf_db) + "\n"
                            
                    else:
                        # not found Check from Error 
                        sql_billing_Error = f"""SELECT DATA_DATE, METER_ID, METER_STREAM_NO 
                                                                FROM amr_billing_data_error
                                                                WHERE DATA_DATE = TO_DATE('{date_polled}', 'DD-MM-YYYY')
                                                                AND METER_ID = '{METERID}' 
                                                                AND METER_STREAM_NO = '{run}'"""
                        billing_Error = fetch_data(sql_billing_Error)
                    
                        if billing_Error:
                            # Check if already not insert
                            # case 2 data already in errpr =  skip # case 3 new error data = insert into error
                            # combine to insert if not exist
                            full_sql_text = full_sql_text + create_SQL_text_insert_Billing_error(METERID, run, current_datetime_upper, date_polled,corrected_polled, uncorrected_polled, avr_pf_polled, avr_tf_polled) + "\n"
                        else:
                            # case  1 new data = insert into billing
                            full_sql_text = full_sql_text + create_SQL_text_insert_Billing(METERID, run, current_datetime_upper, date_polled,corrected_polled, uncorrected_polled, avr_pf_polled, avr_tf_polled) + "\n"
                print(full_sql_text)

                # ddev
                # if full_sql_text: 
                #     with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                #         with connection.cursor() as cursor:
                #             for sql_statement in full_sql_text.split(";"):
                #                 if sql_statement.strip():
                #                     cursor.execute(sql_statement.strip())
                #             connection.commit()
                #             print("Insert data billing successful")
                
                ##### Config Data
                sql_text_config_delete = f"""DELETE FROM AMR_CONFIGURED_DATA WHERE METER_ID = '{METERID}' AND METER_STREAM_NO = '{run}' AND TRUNC(DATA_DATE) = TO_DATE('{date_system}', 'DD-MM-YYYY')"""

                
                sql_text_config_insert = "insert into AMR_CONFIGURED_DATA (DATA_DATE, METER_ID,METER_STREAM_NO, AMR_VC_TYPE,TIME_CREATE, "
                for i in range(0, len(df_mapping)):  
                        
                    sql_text_config_insert+=f" AMR_CONFIG{i+1},"
                sql_text_config_insert+=" CREATED_BY) values ("
                    
                sql_text_config_insert+=f"TO_DATE('{date_system}', 'DD-MM-YYYY'), '{METERID}','{run}','{evc_type}','{current_datetime_upper}',"
                    
                for i in range(0, len(df_mapping)):
                    value = f"'{str(list_of_values_configured[i])}',"
                    if value.strip() == 'NULL,':  # Remove the single quotes and the trailing comma
                        value = "'',"
                    sql_text_config_insert += value
                sql_text_config_insert+="'')"
                print(sql_text_config_delete)
                print(sql_text_config_insert)

                
        
                # ddev
                # with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                #     with connection.cursor() as cursor:
                #         
                #         cursor.execute(sql_text_config_delete)  
                #         
                #         cursor.execute(sql_text_config_insert)  
                #     connection.commit()     
                #     print("Insert data 'config' successful")

            
            successful_message_test.append(METERID) 
        except Exception as e:
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            current_time_error = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            print("Error:", e) 
            print("Error args:", e.args)

            tcp_ip = row[6]
            tcp_port = row[7]
            run = row[3]
            Sitename = row[1]
            METERID = row[4]
            VCtype = row[12]
            
            
            traceback_info = traceback.format_exc()
            error_message = f"Error occurred at {current_time} - TCP IP: {tcp_ip}, Port: {tcp_port}, Run: {run}, METERID: {METERID}. Error: {type(e).__name__}: {str(e)}"
            logger_error.error(error_message)
            
            error_desc = f"{str(e)}"
            max_length = 200

            if len(error_desc) > max_length:
                error_desc = error_desc[:max_length]

            error_desc = error_desc.replace("'", "''")

            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                with connection.cursor() as cursor:    
                    print("INSERT AMR_ERROR successfully")

            traceback.print_exc()
            
            error_message_test.append(METERID) 
            print("Skipping to the next row.")
            
            continue

    if successful_message_test:
        print("All successful Messages:")
        for msg in successful_message_test:
            print(msg)

    # Count occurrences of unique error METERIDs and print them
    counter_1 = Counter(successful_message_test)
    if counter_1:
        print(f"Total unique successful METERIDs: {len(counter_1.keys())}")
        print("All successful METERIDs (unique):")
        for meter_id in counter_1.keys():
            print(meter_id)

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


    
    exit()