import datetime
import struct

def convert_raw_to_value_config(data_type, raw_data):
    try:
        if data_type == "Date":
            # Convert hexadecimal raw data to integer (timestamp)
            raw_data_as_int = int(raw_data, 16)
            # Convert to a datetime object
            #date_object = datetime.datetime.fromtimestamp(raw_data_as_int)
            date_object = datetime.datetime.fromtimestamp(raw_data_as_int, tz=datetime.timezone.utc)
            # Format the date as 'DD-MM-YYYY HH:MM:SS'
            formatted_date = date_object.strftime('%d-%m-%Y %H:%M:%S')
            return formatted_date
        
        elif data_type == "Float":
            # Convert hexadecimal raw data to float
            float_value = struct.unpack('!f', bytes.fromhex(raw_data))[0]
            # Round the float value to 5 decimal places
            rounded_float_value = round(float_value, 5)
            return rounded_float_value
        
        elif data_type == "Ulong":
            # Convert hexadecimal raw data to integer
            return int(raw_data, 16)
        
        else:
            # Return raw data as-is for unsupported types
            return raw_data
    
    except ValueError as e:
        # Handle invalid inputs gracefully
        return f"Error: {e}"

    except Exception as e:
        # Catch any other unexpected errors
        return f"Unexpected Error: {e}"