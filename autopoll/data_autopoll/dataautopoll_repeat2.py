import datetime
import cx_Oracle
import pandas as pd
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



date_system = datetime.datetime.now().strftime('%d-%m-%Y')

# Adjust the query to use bind variables
data_autopoll = f"""
SELECT DISTINCT amr_configured_data.meter_id, amr_field_id.tag_id
FROM amr_configured_data, amr_field_id
WHERE amr_configured_data.meter_id = amr_field_id.meter_id
AND amr_configured_data.data_date =  TO_DATE('{date_system}', 'DD-MM-YYYY')
"""

results_datapoll = fetch_data(data_autopoll)
df_data_autopoll = pd.DataFrame(results_datapoll, columns=['meter_id', 'tag_id'])




data_autopoll_error = f""" SELECT DISTINCT TAG_ID FROM AMR_ERROR where TRUNC(DATA_DATE) =  TO_DATE('{date_system}', 'DD-MM-YYYY') and REPEAT = 2"""
results_datapoll_error = fetch_data(data_autopoll_error)
df_data_autopoll_error = pd.DataFrame(results_datapoll_error, columns=['tag_id'])



df_data_autopoll_cleaned = df_data_autopoll[~df_data_autopoll['tag_id'].isin(df_data_autopoll_error['tag_id'])]

# print("Original data_autopoll:\n", df_data_autopoll)
# print("\nData_autopoll_error:\n", df_data_autopoll_error)
# print("\nCleaned data_autopoll:\n", df_data_autopoll_cleaned)



input_dataautopoll = f"""UPDATE AMR_DATA_AUTOPOLL SET  METER_POLL_REPEAT2 = '{df_data_autopoll_cleaned.shape[0]}', ERROR_REPEAT2 = '{df_data_autopoll_error.shape[0]}' WHERE DATA_DATE =  TO_DATE('{date_system}', 'DD-MM-YYYY') """
with cx_Oracle.connect(username, password, f"{hostname}:{port}/{service_name}") as connection:
                with connection.cursor() as cursor:
                    cursor.execute(input_dataautopoll)
                    connection.commit()
                    print("Insert data 'config_delete' successful")