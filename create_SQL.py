
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
