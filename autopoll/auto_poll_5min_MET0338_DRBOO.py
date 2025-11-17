import argparse
import pandas as pd
import cx_Oracle
import traceback
from datetime import datetime, timedelta
from flask import abort
import socket
import time
import datetime
import logging
from collections import Counter
from convert_modbus import convert_raw_to_value
import math
import os
import sys
from pathlib import Path
import json

# แสดง help list ของสคริปต์
# python "AutoPoll_5min_MET0338_DRBOO.py" --help

# ตัวอย่างการรันสคริปต์
# python AutoPoll_5min_MET0338_DRBOO.py

# บันทึกทั้งไฟล์และฐานข้อมูล
# python AutoPoll_5min_MET0338_DRBOO.py --save-db=true

# บันทึกเฉพาะไฟล์ (ไม่บันทึกฐานข้อมูล)
# python AutoPoll_5min_MET0338_DRBOO.py --save-db=false

# เปลี่ยน meter และ connection
# python AutoPoll_5min_MET0338_DRBOO.py --meterid "METER001" --tcp-ip "192.168.1.100" --tcp-port 502 --save-db=true

# เปลี่ยน interval เป็น 10 นาที
# python AutoPoll_5min_MET0338_DRBOO.py --interval 600 --save-db=false

# save path สำหรับบันทึกไฟล์ polling
# python AutoPoll_5min_MET0338_DRBOO.py --save-path "C:\MyData\Polls"

# Add root directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import modbusdrv
import dotenv
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(filename='autopoll_5min_error.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

communication_traffic = []
change_to_32bit_counter = 0
evc_type = 1  # Default evc_type

def is_polling_disabled():
    """Polling is always enabled"""
    return False

def verifyNumericReturnNULL(value):
    if (isinstance(value, (int, float)) and math.isnan(value)) or value == '' or value == 'None' or value == 'NONE' or value is None:
        return 'NULL'
    return value

def is_valid_date(date_str, date_format='%d-%m-%Y'):
    if all(char == '0' for char in date_str):
        return False
    try:
        datetime.datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False

def convert_to_binary_string(value, bytes_per_value):
    binary_string = bin(value)[2:]
    return binary_string.zfill(bytes_per_value * 8)

def convert_raw_to_value_config(data_type, raw_data):
    data = convert_raw_to_value(data_type, raw_data, "config")
    return data

# Load environment variables
load_dotenv()

username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
# Use a clearer name to avoid confusing the DB host with meter/sim IPs
db_hostname = os.getenv("DB_HOSTNAME")
port = os.getenv("DB_PORT")
service_name = os.getenv("DB_SERVICE")

# DSN for Oracle uses the DB host — kept separate from any meter TCP IP (SIM_IP)
dsn = cx_Oracle.makedsn(db_hostname, port, service_name=service_name)

connection_info = {
    "user": username,
    "password": password,
    "dsn": dsn,
    "min": 1,
    "max": 5,
    "increment": 1,
    "threaded": True
}

# Defer connection pool creation until needed
connection_pool = None

def get_connection_pool():
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = cx_Oracle.SessionPool(**connection_info)
        except cx_Oracle.DatabaseError as e:
            print(f"Database connection failed: {e}")
            print("Please check your database credentials and Oracle client installation.")
            return None
    return connection_pool

def fetch_data(query, params=None):
    pool = get_connection_pool()
    if pool is None:
        return []
    
    try:
        with pool.acquire() as connection:
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

def create_SQL_text_insert_Billing(meterid, run, current_datetime_upper, data_date, corrected, uncorrected, avr_pf, avr_tf):
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

def create_SQL_text_insert_Billing_error(meterid, run, current_datetime_upper, data_date, corrected, uncorrected, avr_pf, avr_tf):
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

def save_to_database(METERID, run, list_of_values_configured, list_of_values_billing, df_mapping, df_mappingbilling, max_day_polled, max_order):
    try:
        current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime('%d-%b-%y %I.%M.%S.%f %p ASIA/BANGKOK')
        current_datetime_upper = current_datetime.upper()
        date_system = datetime.datetime.now().strftime('%d-%m-%Y')

        # Config Data
        sql_text_config_delete = f"""DELETE FROM AMR_CONFIGURED_DATA WHERE METER_ID = '{METERID}' AND METER_STREAM_NO = '{run}' AND TRUNC(DATA_DATE) = TO_DATE('{date_system}', 'DD-MM-YYYY')"""
        sql_text_config_insert = "INSERT INTO AMR_CONFIGURED_DATA (DATA_DATE, METER_ID,METER_STREAM_NO, AMR_VC_TYPE,TIME_CREATE, "
        for i in range(0, len(df_mapping)):
            sql_text_config_insert += f"AMR_CONFIG{i+1},"
        sql_text_config_insert += "CREATED_BY) VALUES ("
        sql_text_config_insert += f"TO_DATE('{date_system}', 'DD-MM-YYYY'), '{METERID}','{run}','{evc_type}','{current_datetime_upper}',"
        for i in range(0, len(df_mapping)):
            value = f"'{str(list_of_values_configured[i])}',"
            if value.strip() == 'NULL,':
                value = "'',"
            sql_text_config_insert += value
        sql_text_config_insert += "'')"

        # Execute config insert
        pool = get_connection_pool()
        if pool is None:
            raise Exception("Database connection not available")
        
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text_config_delete)
                cursor.execute(sql_text_config_insert)
        print("Config data saved to database")

        # Billing Data
        full_sql_text = ""
        for i in range(0, max_day_polled):
            values_subset = list_of_values_billing[(i * max_order):(i * max_order) + max_order]
            date_polled = values_subset[0]
            corrected_polled = verifyNumericReturnNULL(values_subset[1])
            uncorrected_polled = verifyNumericReturnNULL(values_subset[2])
            avr_pf_polled = verifyNumericReturnNULL(values_subset[3])
            avr_tf_polled = verifyNumericReturnNULL(values_subset[4])

            # Check existing data
            sql_billing_DB = f"""SELECT DATA_DATE, CORRECTED_VOL, UNCORRECTED_VOL, AVR_PF, AVR_TF, METER_ID, METER_STREAM_NO 
                                FROM amr_billing_data
                                WHERE DATA_DATE = TO_DATE('{date_polled}', 'DD-MM-YYYY')
                                AND METER_ID = '{METERID}' 
                                AND METER_STREAM_NO = '{run}'"""
            billing_DB = fetch_data(sql_billing_DB)

            if billing_DB:
                date_db = billing_DB[0][0]
                corrected_db = verifyNumericReturnNULL(billing_DB[0][1])
                uncorrected_db = verifyNumericReturnNULL(billing_DB[0][2])
                avr_pf_db = verifyNumericReturnNULL(billing_DB[0][3])
                avr_tf_db = verifyNumericReturnNULL(billing_DB[0][4])

                if (corrected_polled == corrected_db and uncorrected_polled == uncorrected_db and
                    (avr_pf_polled == avr_pf_db or (avr_pf_polled is None and avr_pf_db is None)) and
                    (avr_tf_polled == avr_tf_db or (avr_tf_polled is None and avr_tf_db is None))):
                    pass  # Data matches, do nothing
                else:
                    full_sql_text += create_SQL_text_delete_Billing(METERID, run, date_polled) + "\n"
                    full_sql_text += create_SQL_text_insert_Billing_error(METERID, run, current_datetime_upper, date_polled, corrected_polled, uncorrected_polled, avr_pf_polled, avr_tf_polled) + "\n"
                    full_sql_text += create_SQL_text_insert_Billing_error(METERID, run, current_datetime_upper, date_polled, corrected_db, uncorrected_db, avr_pf_db, avr_tf_db) + "\n"
            else:
                sql_billing_Error = f"""SELECT DATA_DATE, METER_ID, METER_STREAM_NO 
                                        FROM amr_billing_data_error
                                        WHERE DATA_DATE = TO_DATE('{date_polled}', 'DD-MM-YYYY')
                                        AND METER_ID = '{METERID}' 
                                        AND METER_STREAM_NO = '{run}'"""
                billing_Error = fetch_data(sql_billing_Error)

                if billing_Error:
                    full_sql_text += create_SQL_text_insert_Billing_error(METERID, run, current_datetime_upper, date_polled, corrected_polled, uncorrected_polled, avr_pf_polled, avr_tf_polled) + "\n"
                else:
                    full_sql_text += create_SQL_text_insert_Billing(METERID, run, current_datetime_upper, date_polled, corrected_polled, uncorrected_polled, avr_pf_polled, avr_tf_polled) + "\n"

        if full_sql_text:
            with pool.acquire() as connection:
                with connection.cursor() as cursor:
                    for sql_statement in full_sql_text.split(";"):
                        if sql_statement.strip():
                            cursor.execute(sql_statement.strip())
            print("Billing data saved to database")

    except Exception as e:
        logging.error(f"Error saving to database: {e}")

def poll_meter(sitename_poll, runno, meterid, tcp_ip, tcp_port, save_to_db=True):
    try:
        # Hardcoded values for MET0338 DRBOO
        Sitename = sitename_poll
        VCtype = "EVC Type"  # Placeholder
        run = runno
        METERID = meterid
        evc_type = 1  # Assuming evc_type, adjust if needed
        poll_config_set = "0,10,1,10"  # Example, adjust as needed
        poll_billing_set = "0,10,1,10"  # Example, adjust as needed
        CONFIG_ENABLE_set = "11"  # Example
        BILLING_ENABLE_set = "11"  # Example

        # Guard: if someone accidentally passes the DB host as the meter IP, warn them
        try:
            db_host_check = db_hostname
        except NameError:
            db_host_check = None

        if db_host_check and tcp_ip and str(tcp_ip).strip() == str(db_host_check).strip():
            print("Warning: the tcp-ip equals DB_HOSTNAME (DB host).\nMake sure you supplied the meter/SIM IP (SIM_IP) — DB host and meter IP are different.")

        print(f"Connecting to {tcp_ip}:{tcp_port}")

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

        data = {'starting_address_i': [], 'quantity_i': [], 'adjusted_quantity_i': []}
        df_pollRange = pd.DataFrame(data)
        df_pollBilling = pd.DataFrame(data)

        for i in range(1, len(CONFIG_ENABLE_set) + 1):
            if CONFIG_ENABLE_set[i - 1] == '1':
                starting_address_i = starting_address_config_[i]
                quantity_i = quantity_config_[i]
                adjusted_quantity_i = quantity_i - starting_address_i + 1
                data = {'starting_address_i': [starting_address_i],
                        'quantity_i': [quantity_i],
                        'adjusted_quantity_i': [adjusted_quantity_i]}
                df_2 = pd.DataFrame(data)
                df_pollRange = pd.concat([df_pollRange, df_2], ignore_index=True)

        for i in range(1, len(BILLING_ENABLE_set) + 1):
            if BILLING_ENABLE_set[i - 1] == '1':
                starting_address_i = starting_address_[i]
                quantity_i = quantity_[i]
                adjusted_quantity_i = quantity_i - starting_address_i + 1
                data = {'starting_address_i': [starting_address_i],
                        'quantity_i': [quantity_i],
                        'adjusted_quantity_i': [adjusted_quantity_i]}
                df_2 = pd.DataFrame(data)
                df_pollBilling = pd.concat([df_pollBilling, df_2], ignore_index=True)

        sock_i = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_i.settimeout(20)
        sock_i.connect((tcp_ip, int(tcp_port)))
        print("Connected successfully")

        dataframes = {'address_start': [], 'finish': [], 'TX': [], 'RX': []}
        df_Modbus = pd.DataFrame(dataframes)
        df_Modbusbilling = pd.DataFrame(dataframes)

        if int(evc_type) in [5, 8, 9, 10, 12, 17]:
            for _ in range(2):
                modbusdrv.wakeupEVC(sock_i)
                time.sleep(1)

        # Poll Config
        for i in range(0, len(df_pollRange)):
            start_address = int(df_pollRange.loc[i, 'starting_address_i'])
            adjusted_quantity = int(df_pollRange.loc[i, 'adjusted_quantity_i'])
            request_message_i = modbusdrv.modbus_package(slave_id, function_code, start_address, adjusted_quantity)
            communication_traffic_i = []
            communication_traffic_i.append(request_message_i.hex())
            print(f"Poll Config:{i}, start:{start_address}, length:{adjusted_quantity}")
            response_i = modbusdrv.tx_socket(sock_i, request_message_i, 1.8, 1)
            communication_traffic_i.append(response_i.hex())
            if response_i[1:2] != b'\x03':
                raise Exception(f"Error: Unexpected response code from device {communication_traffic_i[1]}!")
            data = {'address_start': [int(start_address)],
                    'finish': [int(start_address + adjusted_quantity)],
                    'TX': [communication_traffic_i[0]],
                    'RX': [communication_traffic_i[1]]}
            df_2 = pd.DataFrame(data)
            df_Modbus = pd.concat([df_Modbus, df_2], ignore_index=True)

        # Poll Billing
        for i in range(0, len(df_pollBilling)):
            start_address = int(df_pollBilling.loc[i, 'starting_address_i'])
            adjusted_quantity = int(df_pollBilling.loc[i, 'adjusted_quantity_i'])
            request_message_i = modbusdrv.modbus_package(slave_id, function_code, start_address, adjusted_quantity)
            communication_traffic_i = []
            communication_traffic_i.append(request_message_i.hex())
            print(f"Poll Billing:{i}, start:{start_address}, length:{adjusted_quantity}")
            response_i = modbusdrv.tx_socket(sock_i, request_message_i, 1.8, 1)
            communication_traffic_i.append(response_i.hex())
            if response_i[1:2] != b'\x03':
                raise Exception(f"Error: Unexpected response code from device {communication_traffic_i[1]}!")
            data = {'address_start': [int(start_address)],
                    'finish': [int(start_address + adjusted_quantity - 1)],
                    'TX': [communication_traffic_i[0]],
                    'RX': [communication_traffic_i[1]]}
            df_2 = pd.DataFrame(data)
            df_Modbusbilling = pd.concat([df_Modbusbilling, df_2], ignore_index=True)

        sock_i.close()

        # Process Config Data
        query = f"select amc.or_der as order1, amc.address as address1, amc.description as desc1, amc.data_type as dtype1 \
        from amr_mapping_config amc \
        where amc.evc_type = '{evc_type}' AND address is not null \
        order by order1"
        cursor = fetch_data(query)
        df_mapping = pd.DataFrame(cursor, columns=['order', 'address', 'desc', 'data_type'])

        list_of_values_configured = []
        for i in range(0, len(df_mapping)):
            address = int(df_mapping.iloc[i, 1])
            data_type = str(df_mapping.iloc[i, 3])
            for j in range(0, len(df_Modbus)):
                address_start = int(df_Modbus.iloc[j, 0])
                address_finish = int(df_Modbus.iloc[j, 1])
                if address >= address_start and address <= address_finish:
                    location_data = (address - address_start) * int(8 / 2)
                    frameRx = (df_Modbus.iloc[j, 3])
                    if data_type == "EVODate":
                        raw_data = frameRx[location_data + 6: location_data + 18]
                    else:
                        raw_data = frameRx[location_data + 6: location_data + 14]
                    list_of_values_configured.append(convert_raw_to_value_config(data_type, raw_data))
                    break
                elif address == 0:
                    list_of_values_configured.append('0')
                    break

        # Process Billing Data
        query = f"SELECT amb.daily ,amb.or_der ,amb.address,amb.description,amb.data_type  FROM amr_mapping_billing amb WHERE amb.evc_type = '{evc_type}' AND address is not null order by amb.daily,amb.or_der"
        cursor = fetch_data(query)
        df_mappingbilling = pd.DataFrame(cursor, columns=['daily', 'or_der', 'address', 'description', 'data_type'])

        list_of_values_billing = []
        check_first_date = 0
        for i in range(len(df_mappingbilling)):
            address = int(df_mappingbilling.iloc[i, 2])
            data_type = str(df_mappingbilling.iloc[i, 4])
            for j in range(0, len(df_Modbusbilling)):
                address_start = int(df_Modbusbilling.iloc[j, 0])
                address_finish = int(df_Modbusbilling.iloc[j, 1])
                if address >= address_start and address <= address_finish:
                    location_data = (address - address_start) * int(8 / 2)
                    frameRx = (df_Modbusbilling.iloc[j, 3])
                    if data_type == "EVODate":
                        raw_data = frameRx[location_data + 6: location_data + 18]
                    else:
                        raw_data = frameRx[location_data + 6: location_data + 14]
                    data_calc = convert_raw_to_value(data_type, raw_data, "billing")
                    if check_first_date == 0:
                        if data_type in ["Date", "EVODate"]:
                            check_first_date = 1
                            firstday_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%d-%m-%Y')
                            if data_calc != firstday_str:
                                raise Exception("First date invalid")
                    list_of_values_billing.append(data_calc)
                    break

        # Prepare data to save
        config_data = {df_mapping.iloc[i, 2]: list_of_values_configured[i] for i in range(len(df_mapping))}
        billing_data = {}
        max_day_polled = df_mappingbilling['daily'].max()
        max_order = df_mappingbilling['or_der'].max()
        for i in range(max_day_polled):
            for j in range(max_order):
                jj = (i * max_order) + j
                key = f"day{i+1}_billing{j+1}_{df_mappingbilling.iloc[j, 3]}"
                billing_data[key] = list_of_values_billing[jj]

        collected_data = {
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "meter_id": METERID,
            "sitename": Sitename,
            "run": run,
            "config_data": config_data,
            "billing_data": billing_data
        }

        # Save to database (only if save_to_db is True)
        if save_to_db:
            save_to_database(METERID, run, list_of_values_configured, list_of_values_billing, df_mapping, df_mappingbilling, max_day_polled, max_order)
            print("💾 Data saved to database")
        else:
            print("📄 Database save skipped (file-only mode)")

        return collected_data

    except Exception as e:
        logging.error(f"Error polling meter: {e}")
        return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Auto Poll MET0338 DRBOO Meter')
    parser.add_argument('--sitename', type=str, default='DRBOO', help='Site name (default: DRBOO)')
    parser.add_argument('--runno', type=int, default=1, help='Run number (default: 1)')
    parser.add_argument('--meterid', type=str, default='MET0338', help='Meter ID (default: MET0338)')
    parser.add_argument('--tcp-ip', type=str, default='192.168.102.192', help='TCP IP address (default: 192.168.102.192)')
    parser.add_argument('--tcp-port', type=int, default=1521, help='TCP port (default: 1521)')
    parser.add_argument('--save-db', type=lambda x: (str(x).lower() in ['true', '1', 'yes', 'on']), 
                       default=False, help='Save data to database (default: False, save txt only, accepts: true/false, 1/0, yes/no, on/off)')
    parser.add_argument('--save-path', type=str, default='.', help='Path to save the txt file (default: current directory)')
    parser.add_argument('--interval', type=int, default=300, help='Polling interval in seconds (default: 300 = 5 minutes)')

    args = parser.parse_args()

    sitename = args.sitename
    runno = args.runno
    meterid = args.meterid
    tcp_ip = args.tcp_ip
    tcp_port = args.tcp_port
    save_to_db = args.save_db
    poll_interval = args.interval
    save_path = args.save_path

    print(f"🚀 Starting Auto Poll for {meterid} at {sitename}")
    print(f"📡 Connection: {tcp_ip}:{tcp_port}")
    print(f"💾 Save to DB: {'Yes' if save_to_db else 'No (File only)'}")
    print(f"📁 Save Path: {save_path}")
    print(f"⏱️  Poll Interval: {poll_interval} seconds")
    print("="*60)

    while True:
        if is_polling_disabled():
            # Calculate time until next allowed polling time (6:00 AM)
            now = datetime.datetime.now()
            if now.time() >= datetime.time(22, 30):
                # If after 22:30, sleep until 6:00 tomorrow
                next_poll_time = datetime.datetime.combine(now.date() + datetime.timedelta(days=1), datetime.time(6, 0))
            else:
                # If before 6:00, sleep until 6:00 today
                next_poll_time = datetime.datetime.combine(now.date(), datetime.time(6, 0))
            
            sleep_seconds = (next_poll_time - now).total_seconds()
            print(f"🛑 Polling disabled during 22:30-06:00. Sleeping until {next_poll_time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(sleep_seconds)
            continue
        
        print(f"🔄 Polling at {datetime.datetime.now()}")
        data = poll_meter(sitename, runno, meterid, tcp_ip, tcp_port, save_to_db)
        if data:
            # Save to file with date
            now = datetime.datetime.now()
            date_str = now.strftime('%Y%m%d')
            time_str = now.strftime('%H_%M')
            filename = f"data_{meterid}_{sitename}_Date_{date_str}_Time_{time_str}.txt"
            os.makedirs(save_path, exist_ok=True)
            full_path = os.path.join(save_path, filename)
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(f"Timestamp: {data['timestamp']}\n")
                f.write(f"Meter ID: {data['meter_id']}\n")
                f.write(f"Sitename: {data['sitename']}\n")
                f.write(f"Run: {data['run']}\n\n")
                
                # Write Billing Data in table format
                f.write("BILLING DATA\n")
                f.write("=" * 120 + "\n")
                f.write(f"{'Time Stamp':<20} {'Uncorrected Volume':<25} {'Corrected Volume':<25} {'Pressure Daily Average':<30} {'Temperature Daily Average':<30}\n")
                f.write("=" * 120 + "\n")
                
                # Parse billing data
                billing_data = data['billing_data']
                days = {}
                for key, value in billing_data.items():
                    # Extract day number from key like "day1_billing1_..."
                    if key.startswith('day'):
                        day_num = key.split('_')[0]
                        if day_num not in days:
                            days[day_num] = {}
                        days[day_num][key] = value
                
                # Write each day's data
                for day_num in sorted(days.keys()):
                    day_data = days[day_num]
                    timestamp = ""
                    uncorrected = ""
                    corrected = ""
                    pressure = ""
                    temperature = ""
                    
                    for key, value in day_data.items():
                        if 'DATE' in key.upper() or 'TIME' in key.upper():
                            timestamp = str(value)
                        elif 'UNCORRECTED' in key.upper():
                            uncorrected = str(value)
                        elif 'CORRECTED' in key.upper():
                            corrected = str(value)
                        elif 'AVR_PF' in key.upper() or 'PRESSURE' in key.upper():
                            pressure = str(value)
                        elif 'AVR_TF' in key.upper() or 'TEMPERATURE' in key.upper() or 'TEMP' in key.upper():
                            temperature = str(value)
                    
                    f.write(f"{timestamp:<20} {uncorrected:<25} {corrected:<25} {pressure:<30} {temperature:<30}\n")
                
                f.write("=" * 120 + "\n\n")
                
                # Write Config Data in table format
                f.write("CONFIG DATA\n")
                f.write("=" * 180 + "\n")
                # Column headers
                headers = ['Date and Time', 'Imp.w', 'Pb', 'Tb', 'Prd', 'Trd', 'SG', 'CO2', 'N2', 'Pressure', 'Temperature', 'Z Ration', 'Zf', 'Cf', 'Qm', 'Qb', 'Low Battery Alarm']
                header_line = ""
                for header in headers:
                    header_line += f"{header:<12} "
                f.write(header_line + "\n")
                f.write("=" * 180 + "\n")
                
                # Write config data row
                config_data = data['config_data']
                config_values = []
                
                # Map config data to columns (adjust based on actual data structure)
                for header in headers:
                    value = "N/A"
                    for key, val in config_data.items():
                        if header.upper().replace(' ', '').replace('.', '') in key.upper().replace(' ', '').replace('_', ''):
                            value = str(val)
                            break
                    config_values.append(value)
                
                # Write the row
                row_line = ""
                for val in config_values:
                    row_line += f"{val:<12} "
                f.write(row_line + "\n")
                f.write("=" * 180 + "\n\n")
                
                # Write raw config data for reference
                f.write("RAW CONFIG DATA:\n")
                f.write("-" * 60 + "\n")
                for key, value in config_data.items():
                    f.write(f"  {key}: {value}\n")
            print(f"✅ Data saved to {full_path}")
        else:
            logging.error("Failed to poll data")

        # Wait for next poll
        print(f"⏰ Waiting {poll_interval} seconds until next poll...")
        time.sleep(poll_interval)