# This script is upload net new eod data for all stocks to DB
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, BigInteger, Float, DateTime
from sqlalchemy import create_engine
from io import StringIO
import datetime
import sys
import socket

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

def get_rs_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker from industry_groups"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker'])
    RS_list = tuple(df.ticker.unique())
    return RS_list

def get_dates_onlyRS_tickers(conn, rss_list):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker, timestamp from usstockseod where ticker in %s" % (rss_list,)
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker', 'timestamp'])
    t_list = list(df.ticker.unique())
    count=0
    dct_dates ={}
    total = len(t_list)
    print(total)

    for ticker in t_list:
        count = count + 1
        print(count)
        t_df = df.loc[df['ticker'] == ticker]
        dct_dates[ticker] = t_df['timestamp'].max() + datetime.timedelta(days=1)

    for sym, dte in dct_dates.items():
        dct_dates[sym] = pd.Timestamp(dct_dates[sym])
        dct_dates[sym] = dct_dates[sym].to_pydatetime()
        dct_dates[sym] = dct_dates[sym].strftime("%Y%m%d")

    return dct_dates


def get_dates_missed_tickers(conn, miss_list):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker, timestamp from usstockseod where ticker in %s" % (miss_list,)
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker', 'timestamp'])
    t_list = list(df.ticker.unique())
    count=0
    dct_dates ={}
    total = len(t_list)
    print(total)

    for ticker in t_list:
        count = count + 1
        print(count)
        t_df = df.loc[df['ticker'] == ticker]
        dct_dates[ticker] = t_df['timestamp'].max() + datetime.timedelta(days=1)

    for sym, dte in dct_dates.items():
        dct_dates[sym] = pd.Timestamp(dct_dates[sym])
        dct_dates[sym] = dct_dates[sym].to_pydatetime()
        dct_dates[sym] = dct_dates[sym].strftime("%Y%m%d")

    return dct_dates


def get_dates_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker, timestamp from usstockseod"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker', 'timestamp'])
    t_list = list(df.ticker.unique())
    count=0
    dct_dates ={}
    total = len(t_list)
    print(total)

    for ticker in t_list:
        count = count + 1
        print(count)
        t_df = df.loc[df['ticker'] == ticker]
        dct_dates[ticker] = t_df['timestamp'].max() + datetime.timedelta(days=1)

    for sym, dte in dct_dates.items():
        dct_dates[sym] = pd.Timestamp(dct_dates[sym])
        dct_dates[sym] = dct_dates[sym].to_pydatetime()
        dct_dates[sym] = dct_dates[sym].strftime("%Y%m%d")

    return dct_dates


def get_historical_data(dct_tickers):
    fdf = pd.DataFrame()
    columns = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
    excp = []
    count = 1
    verr = []

    for sym, dte in dct_tickers.items():
        print("Downloading symbol: %s..." % sym, count)
        count = count + 1
        # Construct the message needed by IQFeed to retrieve data

        message = "HDT,%s,%s,20250101\n" % (sym, dte)
        message = bytes(message, encoding='utf-8')

        # Open a streaming socket to the IQFeed server locally
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        # Send the historical data request
        # message and buffer the data
        sock.sendall(message)
        data = read_historical_data_socket(sock)
        sock.close()

        if "!NO_DATA!" in data:
            print("no data for %s " % sym, count)
            excp.append(sym)
            continue
        # Remove all the endlines and line-ending
        # comma delimiter from each record
        print(data)
        data = str(data)
        data = "".join(data.split("\r"))
        data = data.replace(",\n", "\n")[:-1]
        dd_ls1 = list(data.split('\n'))
        dd_ls2 = []
        [dd_ls2.append(i.split(',')) for i in dd_ls1]
        try:
            ddf = pd.DataFrame(dd_ls2, columns=columns)
        except ValueError:
            print("connect error and no value for %s" % sym, count)
            verr.append(sym)
            continue
        else:
            ddf.insert(0, 'Ticker', sym)
            fdf = pd.concat([fdf, ddf], ignore_index=True)
            del ddf

    print("no data for these tickers:")
    print(excp)

    print("no connection so no value for these tickers")
    print(verr)

    return fdf

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

def copy_from_stringio(conn, dff, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    dff.to_csv(buffer, index=False, header=False)

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

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    mis_list = ('VRE', 'SG', 'VABS')
    date_tickers = get_dates_missed_tickers(con, mis_list)

    #rs_list = get_rs_tickers(con)
    #date_tickers = get_dates_onlyRS_tickers(con,rs_list)

    #date_tickers = get_dates_tickers(con)


    up_df = get_historical_data(date_tickers)

    copy_from_stringio(con, up_df, "usstockseod")

    con.close()




