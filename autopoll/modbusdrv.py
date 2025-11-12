import time
def computeCRC(data):
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return crc.to_bytes(2, byteorder="little")

def modbus_package(slave_id, fn_code, address_start, length):
    modbus_tx =  bytearray([
        slave_id,
        fn_code,
        (address_start >> 8) & 0xFF,
        address_start & 0xFF,
        (length >> 8) & 0xFF,
        length & 0xFF,
        ])
    crc = computeCRC(modbus_tx)
    modbus_tx += crc
    return modbus_tx

def tcpsocketconnections(ip, port):
    pass

def wakeupEVC(sock_i):
    slave_id_1 = 0x01
    function_code_1 = 0x03
    starting_address_1 = 0x0004
    quantity_1 = 0x0002
    request_Modbus= bytearray([
        slave_id_1,
        function_code_1,
        (starting_address_1 >> 8) & 0xFF,
        starting_address_1 & 0xFF,
        (quantity_1 >> 8) & 0xFF,
        quantity_1 & 0xFF,
        ])
            
    crc_1 = computeCRC(request_Modbus)
    #print(f"crc_1: {crc_1}")

    request_Modbus += crc_1

    for _ in range(2):  
        sock_i.send(request_Modbus)
        #print(sock_i)
        time.sleep(1)
    response = sock_i.recv(4096)

def Tx_modbus(sock, slave_id, fn_code, address_start, length, delay_between_frame = 1.5):
    request_Modbus = modbus_package(slave_id, fn_code, address_start, length)
    return tx_socket(sock, request_Modbus, delay_between_frame)


def tx_socket(sock, request_Modbus, delay_between_frame = 1.5, verbose = 0):
    for attempt in range(3):  # ลองส่งและรับข้อมูลทั้งหมด 3 ครั้ง
        try:
            # send Tx
            sock.send(request_Modbus)
            #print(f"Tx_{attempt}: {request_Modbus.hex()}")
            if verbose == 1: print(f"Tx_{attempt}: {' '.join(f'{b:02X}' for b in request_Modbus)}")
            time.sleep(delay_between_frame)
            # Receive Rx
            response = sock.recv(4096)
            if verbose == 1: print(f"    Rx: {' '.join(f'{b:02X}' for b in response)}")
            
            # Poll OK 
            return response
        
        except Exception as e:
            # Error
            print(f"Attempt {attempt + 1}: Error occurred - {e}")
    
    # ถ้าครบ 3 ครั้งแล้วยังล้มเหลว return "fail"
    print("Failed to communicate after 3 attempts.")
    return "fail"


if __name__ == '__main__':
    pass