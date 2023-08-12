# This script is upload net new eod data for all stocks to DB - may be not required

import socket
import iqfeedTest as iq

def set_protocol(sock):

    print("setting protocol:..." )
    message = "S,SET PROTOCOL,6.1\n\r"
    message = bytes(message, encoding='utf-8')

        # Open a streaming socket to the IQFeed server locally
    sock.connect((host, port))
        # Send the historical data request
        # message and buffer the data
    sock.sendall(message)
    data = read_historical_data_socket(sock)


    return data


def get_historical_data(sock):
    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    print("Downloading symbol:..." )
    message = "EDS,1,6,20220602\n"
    message = bytes(message, encoding='utf-8')

        # Open a streaming socket to the IQFeed server locally

    #sock.connect((host, port))

        # Send the historical data request
        # message and buffer the data
    sock.sendall(message)
    data = read_historical_data_socket(sock)
    sock.close()

    return data


def read_historical_data_socket(sock, recv_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    while True:
        data = str(sock.recv(recv_buffer), encoding='utf-8')
        buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break

        if "SYNTAX" in buffer:
            break

        if "ERROR" in buffer:
            break

        if "CURRENT" in buffer:
            break
    # Remove the end message string
    #buffer = buffer[:-12]
    return buffer



if __name__ == '__main__':
    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    iq.launch_service()
    pro=set_protocol(sock)
    print(pro)

    up_df = get_historical_data(sock)

    print(up_df)