
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



def convert_raw_to_value_hourly(data_type, raw_data):
    if data_type == "Hour":
        raw_data_as_int = int(raw_data, 16)
        
        
        date_object = datetime.datetime.utcfromtimestamp(raw_data_as_int)
        
        # Format to hour and date
        formatted_date = date_object.strftime('%H:00 on %d-%m-%Y') 
        
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


# logger_error = logging.getLogger('error_logger')
# logger_error.setLevel(logging.ERROR)

# # สร้าง Handler เพื่อบันทึกข้อความ error ลงในไฟล์
# log_file_error = 'C:\\Users\\Administrator\\Desktop\\Autopoll\\Actaris(G1).log'
# file_handler_error = logging.FileHandler(log_file_error)
# formatter_error = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
# file_handler_error.setFormatter(formatter_error)
# logger_error.addHandler(file_handler_error)
query = """
            SELECT *
FROM (
    SELECT
        AMR_PL_GROUP.PL_REGION_ID as region,
        AMR_FIELD_ID.TAG_ID as Sitename,
        AMR_FIELD_METER.METER_NO_STREAM as NoRun,
        AMR_FIELD_METER.METER_STREAM_NO as RunNo,
        AMR_FIELD_METER.METER_ID as METERID,
        AMR_VC_TYPE.VC_NAME as VCtype,
        AMR_FIELD_ID.SIM_IP as IPAddress,
        AMR_PORT_INFO.PORT_NO as port,
        amr_vc_type.vc_name as vc_name,
        amr_poll_range_hourly.poll_hourly as poll_hourly,
        amr_poll_range_hourly.poll_hourly_enable as poll_hourly_enable,
        AMR_VC_TYPE.id as evctype,
        amr_field_meter.modbus_id as modbus_id,
        AMR_PORT_INFO.ID as port_id
    FROM
        amr_poll_range_hourly,
        AMR_FIELD_ID,
        AMR_USER,
        AMR_FIELD_CUSTOMER,
        AMR_FIELD_METER,
        AMR_PL_GROUP,
        AMR_VC_TYPE,
        AMR_PORT_INFO
    WHERE
        AMR_FIELD_METER.METER_AUTO_ENABLE = 1 AND
        AMR_FIELD_ID.FIELD_ID = AMR_PL_GROUP.FIELD_ID AND
        AMR_FIELD_ID.METER_ID = AMR_USER.USER_GROUP AND
        AMR_FIELD_ID.CUST_ID = AMR_FIELD_CUSTOMER.CUST_ID AND
        AMR_FIELD_ID.METER_ID = AMR_FIELD_METER.METER_ID AND
        AMR_VC_TYPE.ID = AMR_FIELD_METER.METER_STREAM_TYPE AND
        AMR_FIELD_METER.METER_PORT_NO = AMR_PORT_INFO.ID AND
        amr_poll_range_hourly.evc_type = AMR_VC_TYPE.id AND
        amr_vc_type.id LIKE '10' 
        
    ORDER BY
        AMR_FIELD_ID.TAG_ID ASC, port
)


                                        
                
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
     
        
        tcp_ip = row[6]
        tcp_port = row[7]
        run = row[3]
        METERID = row[4]
        poll_hourly = row[9]
        print(poll_hourly)
        
        poll_hourly_enable = row[10]
        print(poll_hourly_enable)
        print("G4")
        print(tcp_ip)
        print(tcp_port)
        evc_type = row[11]
        print(evc_type)
        print(METERID)
        completed_sets += 1
        modbus_id = row[12]
        port_id = row[13]
        poll_hourly_list = [int(x) for x in poll_hourly.split(',')]
        poll_hourly_enable_list = poll_hourly_enable.split(',')
        pairs_hourly = [(poll_hourly_list[i], poll_hourly_list[i+1]) for i in range(0, len(poll_hourly_list), 2)]
        slave_id = int(1)
        function_code = int(3)
        # Prepare dataframes for billing and config
        data= {'starting_address_i': [], 'quantity_i': [], 'adjusted_quantity_i': []}
        
        df_pollhourly = pd.DataFrame(data)

        # Populate the billing dataframe
        # print("\nhourly Pairs:")
        for index, value in enumerate(poll_hourly_enable_list):
            if value == '1' and index < len(pairs_hourly):
                starting_address_i = int(pairs_hourly[index][0])  # Convert to integer
                quantity_i = int(pairs_hourly[index][1])          # Convert to integer
                adjusted_quantity_i = quantity_i - starting_address_i + 1
                data = {'starting_address_i': [starting_address_i], 
                        'quantity_i': [quantity_i], 
                        'adjusted_quantity_i': [adjusted_quantity_i]}
                df_2 = pd.DataFrame(data)
                df_pollhourly = pd.concat([df_pollhourly, df_2], ignore_index=True)
                # print("df_pollhourly", df_pollhourly)



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
        
        df_Modbushourly = pd.DataFrame(dataframes)
        # print(df_data)
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
        
        
        if int(evc_type) in [5, 8, 9, 10]:
                                    
                                

            for _ in range(2):  
                sock_i.send(request_Actaris)
                print(sock_i)
                time.sleep(0.4)
            response = sock_i.recv(4096)


        if int(evc_type) == 12:
            if int(tcp_port) != 2101:
                
                for _ in range(2):
                    sock_i.send(request_Actaris)
                    print(sock_i)
                    time.sleep(0.5)
                response = sock_i.recv(4096)
            
        for i in range(0, len(df_pollhourly)):
                                
                                
            if int(tcp_port) == 2101 and int(port_id) in [15, 16]:

                sock_i = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_i.settimeout(5)
                sock_i.connect((tcp_ip, int(tcp_port)))
                
            
            start_address = int(df_pollhourly.loc[i,'starting_address_i'])
            
            adjusted_quantity = int(df_pollhourly.loc[i,'adjusted_quantity_i'])
        
            

            request_message_i = bytearray(
            [int(modbus_id), function_code, start_address >> 8, start_address & 0xFF, adjusted_quantity >> 8, adjusted_quantity & 0xFF])
            crc_i = computeCRC(request_message_i)
            
            request_message_i += crc_i.to_bytes(2, byteorder="big")

            
            
            
            communication_traffic_i = []
        
            
            communication_traffic_i.append(request_message_i.hex())
            billing_safe_tx = f"hourly_TX: {communication_traffic_i[0]}"
            # print("tx.hourly",communication_traffic_i[0])

            
            
            sock_i.send(request_message_i)
            time.sleep(2)
            response_i = sock_i.recv(1024)
        
            
            
            
            
            communication_traffic_i.append(response_i.hex())
            
            billing_safe = f"hourly_RX: {communication_traffic_i[1]}"
            # print("rx.hourly",communication_traffic_i[1])
               
            
           
            if response_i[1:2] != b'\x03':
                
                abort(400, f"Error: Unexpected response code from device {communication_traffic_i[1]} !")
            else:
                pass
        
            
            # sock_i.close()
            # print(communication_traffic_i)
            data = {
                'address_start': [int(start_address)],
                'finish': [int(start_address+adjusted_quantity-1)],
                'TX': [communication_traffic_i[0]],
                'RX': [communication_traffic_i[1]]
            }
            # print(data)
            df_2 = pd.DataFrame(data)
            df_Modbushourly = pd.concat([df_Modbushourly, df_2], ignore_index=True)
            # print(df_Modbushourly)

            


            
            query = f"""
            SELECT amb.hourly ,amb.or_der ,amb.address,amb.description,amb.data_type  FROM amr_mapping_hourly amb WHERE amb.evc_type = '{evc_type}' AND address is not null order by amb.hourly
            ,amb.or_der
            """
            poll_resultsbilling = fetch_data(query)
            # print(poll_resultsbilling)
            df_mappingbilling = pd.DataFrame(poll_resultsbilling, columns=['hourly','or_der', 'address', 'description', 'data_type'])
            
            
            
            list_of_values_billing = []
            for i in range(0, len(df_mappingbilling)):
                    
                address = int(df_mappingbilling.iloc[i,2])
                
                data_type = str(df_mappingbilling.iloc[i,4])
                
                #print("AA", address, address_start, address_finish)

                # print(i, df_mappingbilling.loc[i])
                for j in range(0,len(df_Modbushourly)):
                    address_start = int(df_Modbushourly.iloc[j,0])
                    address_finish = int(df_Modbushourly.iloc[j,1])
                
                    if address >= address_start and address <= address_finish:
                        # print(address)
                        # print(address_start, address_finish)
                        location_data = (address - address_start)*int(8/2)
                        # print(location_data)
                        frameRx = (df_Modbushourly.iloc[j,3])
                    
                        raw_data = frameRx[location_data + 6: location_data + 14]
                        # print(raw_data)
                        
                        list_of_values_billing.append(convert_raw_to_value_hourly(data_type, raw_data))   
                        # print("type", data_type ,"raw", raw_data, "=",  convert_raw_to_value(data_type,raw_data))
                        break


            # print(list_of_values_billing)      
            processed_values_hourly = []

            # Iterate over each item in the list_of_values_billing
            for item in list_of_values_billing:
                # Check if the item is a string and contains " on "
                if isinstance(item, str) and " on " in item:
                    # Split the item into time and date parts
                    time_part, date_part = item.split(" on ")
                    
                    # Extract the hour part by splitting the time part and taking the hour
                    hour_part = time_part.split(":")[0].strip()
                    
                    # Convert the date part to the desired format (YYYY-MM-DD)
                    date_part = pd.to_datetime(date_part.strip(), format="%d-%m-%Y").strftime('%Y-%m-%d')
                    
                    # Append the zero-padded hour part to processed_values_hourly
                    processed_values_hourly.append(hour_part.zfill(2))
                    
                    # Append the date part to processed_values_hourly
                    processed_values_hourly.append(date_part)
                else:
                    # If the item is not a string or doesn't contain " on ", simply append it to processed_values_hourly
                    processed_values_hourly.append(item)

            # Output the processed list
            # print(processed_values_hourly)


            current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime('%d-%b-%y %I.%M.%S.%f %p ASIA/BANGKOK')
            current_datetime_upper = current_datetime.upper()
            evc_type = evc_type
            query = """
            SELECT amh.hourly ,amh.or_der ,amh.address,amh.description,amh.data_type  FROM amr_mapping_hourly amh WHERE amh.evc_type = :evc_type AND address is not null order by amh.hourly
            ,amh.or_der
            """
            poll_resultsbilling = fetch_data(query, params={"evc_type": evc_type})
            # print(poll_resultsbilling)
            df_mappingbilling = pd.DataFrame(poll_resultsbilling, columns=['hourly','or_der', 'address', 'description', 'data_type'])
            # print(df_mappingbilling)
            sql_texts = []
            for i in range(0, len(processed_values_hourly), 6):
                
                values_subset = processed_values_hourly[i:i+6]
                original_date_str = values_subset[1]  
                date_obj = datetime.datetime.strptime(original_date_str, '%Y-%m-%d')
                formatted_date_str = date_obj.strftime('%d-%m-%Y')
                
                sql_text_billing_insert = f"""
                    MERGE INTO AMR_BILLING_HOURLY_DATA target
                    USING (
                        SELECT '{METERID}' AS METER_ID, '{run}' AS METER_STREAM_NO, TO_DATE('{formatted_date_str}', 'DD-MM-YYYY') AS DATA_DATE, '{values_subset[0]}' AS DATA_HOUR,
                            '{current_datetime_upper}' AS CREATED_TIME,
                            {values_subset[2]} AS UNCORRECTED_VOL, {values_subset[3]} AS CORRECTED_VOL, {values_subset[4]} AS AVR_PF, {values_subset[5]} AS AVR_TF
                        FROM DUAL
                    ) source
                    ON (
                        target.METER_ID = source.METER_ID
                        AND target.METER_STREAM_NO = source.METER_STREAM_NO
                        AND target.DATA_DATE = source.DATA_DATE
                        AND target.DATA_HOUR = source.DATA_HOUR
                    )
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.CREATED_TIME = source.CREATED_TIME,
                            target.UNCORRECTED_VOL = source.UNCORRECTED_VOL,
                            target.CORRECTED_VOL = source.CORRECTED_VOL,
                            target.AVR_PF = source.AVR_PF,
                            target.AVR_TF = source.AVR_TF
                    WHEN NOT MATCHED THEN
                        INSERT (METER_ID, METER_STREAM_NO, DATA_DATE, DATA_HOUR, CREATED_TIME, UNCORRECTED_VOL, CORRECTED_VOL, AVR_PF, AVR_TF)
                        VALUES (source.METER_ID, source.METER_STREAM_NO, source.DATA_DATE, source.DATA_HOUR, source.CREATED_TIME, source.UNCORRECTED_VOL, source.CORRECTED_VOL, source.AVR_PF, source.AVR_TF);
                """

                
                sql_texts.append(sql_text_billing_insert.strip())
          
            full_sql_text = "\n".join(sql_texts)
            # print("full_sql_text", full_sql_text)
            
            with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                with connection.cursor() as cursor:
                    for sql_statement in full_sql_text.split(";"):
                        if sql_statement.strip():
                            cursor.execute(sql_statement.strip())
                    connection.commit()
                    print("Insert data billing successful")

            

            

            

            



        successful_rows += 1
        print(f"ชุดที่ {completed_sets} เสร็จสิ้นแล้ว\n")
        successful_message_test.append(METERID) 
    except Exception as e:
        current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime('%d-%b-%y %I.%M.%S.%f %p ASIA/BANGKOK').upper()
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_time_error = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        
        tcp_ip = row[6]
        tcp_port = row[7]
        run = row[3]
        Sitename = row[1]
        METERID = row[4]
        VCtype = row[11]



        traceback.print_exc()

        error_desc = f"{str(e)}"
        max_length = 200

        if len(error_desc) > max_length:
            error_desc = error_desc[:max_length]

        sql_text_billing_insert = f"""
            INSERT INTO AMR_HOURLY_ERROR (
                METER_ID, METER_STREAM_NO, DATA_DATE, TAG_ID, ERROR_DESC, EVC_TYPE,TIME_CREATE
            ) VALUES (
                '{METERID}', '{run}', TO_DATE('{current_time_error}', 'DD-MM-YYYY HH24:MI:SS'), '{Sitename}', '{error_desc}', '{VCtype}','{current_datetime}'
            )
        """

        with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text_billing_insert)
                connection.commit()
                
                print("INSERT AMR_ERROR successfully")

        error_message_test.append(METERID) 
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


log_file_info = 'C:\\Users\\Administrator\\Desktop\\autopoll\\AutopollHourly\\totalhourly.log'
file_handler_info = logging.FileHandler(log_file_info)
formatter_info = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
file_handler_info.setFormatter(formatter_info)
logger_info.addHandler(file_handler_info)

logger_info.info(f"Actaris(G4) : Total sets processed: {total_sets}, Successful rows: {successful_rows}, Errors: {error_count}") 




logger_info_site = logging.getLogger('info_logger_site')
logger_info_site.setLevel(logging.INFO)
log_file_info_site = 'C:\\Users\\Administrator\\Desktop\\autopoll\\AutopollHourly\\total_site.log'
file_handler_info_site = logging.FileHandler(log_file_info_site)
formatter_info_site = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
file_handler_info_site.setFormatter(formatter_info_site)
logger_info_site.addHandler(file_handler_info_site)

logger_info_site.info(f"Actaris(G4) : Total site processed: {total_site}, Successful rows: {len(counter_1.keys())}, Errors: {len(counter.keys())}") 




if __name__ == '__main__':
    
    exit()