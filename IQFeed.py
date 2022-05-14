# This script is used to get historical data for a list of symbols from iq feed
import pandas as pd
import socket
import sys
import psycopg2
from io import StringIO


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
    # Remove the end message string
    buffer = buffer[:-12]
    return buffer


def get_tickers_list():
    l_tickers = ['SPY', 'AAPL', 'AMZN']
    return l_tickers


def get_historical_data(l_tickers):
    fdf = pd.DataFrame()
    columns = ["Timestamp", "Open", "Low", "High", "Close", "Volume", "Open Interest"]

    for sym in l_tickers:
        print("Downloading symbol: %s..." % sym)
        # Construct the message needed by IQFeed to retrieve data
        message = "HDT,%s,19000101,20250101\n" % sym
        message = bytes(message, encoding='utf-8')

        # Open a streaming socket to the IQFeed server locally
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        # Send the historical data request
        # message and buffer the data
        sock.sendall(message)
        data = read_historical_data_socket(sock)
        sock.close()

        # Remove all the endlines and line-ending
        # comma delimiter from each record
        data = str(data)
        data = "".join(data.split("\r"))
        data = data.replace(",\n", "\n")[:-1]
        dd_ls1 = list(data.split('\n'))
        dd_ls2 = []
        [dd_ls2.append(i.split(',')) for i in dd_ls1]
        ddf = pd.DataFrame(dd_ls2, columns=columns)
        ddf.insert(0, 'Ticker', sym)
        fdf = pd.concat([fdf, ddf], ignore_index=True)
        del ddf

    fdf = fdf.astype(
        {'Ticker': str, 'Timestamp': str, 'Open': float, 'Low': float, 'High': float, 'Close': float, 'Volume': int,
         'Open Interest': int})

    fdf['Timestamp'] = pd.to_datetime(fdf['Timestamp'])

    return fdf


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def copy_from_stringio(conn, dff, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    dff.to_csv(buffer, index_label='id', header=False)

    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()


if __name__ == '__main__':
    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    # Connection parameters
    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    list_tickers = get_tickers_list()
    df = get_historical_data(list_tickers)

    con = connect(param_dic)
    copy_from_stringio(con, df, "usstockseod")
    con.close()
