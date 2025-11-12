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

def build_request_message(slave_id, function_code, starting_address, quantity):
    request_message = bytearray([
        slave_id,
        function_code,  
        starting_address >> 8,
        starting_address & 0xFF,
        quantity >> 8,
        quantity & 0xFF,
    ])

    crc = computeCRC(request_message)
    request_message += crc
    return request_message

def format_tx_message(slave_id, function_code, starting_address, quantity, data):
    tx_message = bytearray([
        slave_id,          
        function_code,       
        starting_address >> 8, starting_address & 0xFF,  
        quantity >> 8, quantity & 0xFF,                 
        len(data) // 1    
    ])
    tx_message.extend(data)
    
    crc = computeCRC(tx_message)
    tx_message += crc  

    return tx_message