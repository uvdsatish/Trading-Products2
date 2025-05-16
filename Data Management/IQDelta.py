# This script is`Daily, KeyindicatorsPopulation_Delta, Plurality-RS-Upload, update_excel_RS, plurality1_plots
import pandas as pd
import psycopg2
from io import StringIO
import datetime
import sys
import socket
import iqfeedTest as iq
import time


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
    postgreSQL_select_Query = "select ticker, last_timestamp from max_date_price_view where ticker in %s" % (rss_list,)
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker', 'last_timestamp'])

    # increment timestamp by 1 day
    df['next_timestamp'] = df['last_timestamp'] + datetime.timedelta(days=1)

    # convert the timestamp to date string
    df['next_timestamp_dt'] = pd.to_datetime(df['next_timestamp'])
    df['next_timestamp_date'] = df['next_timestamp_dt'].dt.strftime('%Y%m%d')

    df.drop(['last_timestamp', 'next_timestamp', 'next_timestamp_dt'], axis=1, inplace=True)


    return df

def get_all_tickers_defaultDates(conn):

    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker from all_tickers"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker'])

    df['def_date'] = pd.Timestamp('1950-01-01')
    df['date'] = df['def_date'].dt.strftime('%Y%m%d')

    df.drop(['def_date'], axis=1, inplace=True)


    return df


def get_dates_all_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker, last_timestamp from max_date_price_view"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker', 'last_timestamp'])
    # increment timestamp by 1 day
    df['next_timestamp'] = df['last_timestamp'] + datetime.timedelta(days=1)

    # convert the timestamp to date string
    df['next_timestamp_dt'] = pd.to_datetime(df['next_timestamp'])
    df['next_timestamp_date'] = df['next_timestamp_dt'].dt.strftime('%Y%m%d')

    df.drop(['last_timestamp', 'next_timestamp', 'next_timestamp_dt'], axis=1, inplace=True)


    return df

def update_date_tickers_all(def_df,df):
    #merge data frames,overriding overlaps

    merged_df = pd.merge(def_df,df,on='ticker', how='outer')

    merged_df['next_timestamp_date'] = merged_df['next_timestamp_date'].fillna(merged_df['date'])

    merged_df.drop(['date'], axis=1, inplace=True)

    merged_df = merged_df.drop(index=0)

    merged_dates = merged_df.set_index('ticker')['next_timestamp_date'].to_dict()

    return merged_dates


def update_date_tickers_some(def_df,df):
    #merge data frames,overriding overlaps

    merged_df = pd.merge(def_df,df,on='ticker', how = "inner")

    merged_df['next_timestamp_date'] = merged_df['next_timestamp_date'].fillna(merged_df['date'])

    merged_df.drop(['date'], axis=1, inplace=True)

    merged_df = merged_df.drop(index=0)

    merged_dates = merged_df.set_index('ticker')['next_timestamp_date'].to_dict()

    return merged_dates


def get_historical_data(dct_tickers):
    fdf = pd.DataFrame()
    columns = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
    excp = []
    count = 1
    verr = []

    for sym, dte in dct_tickers.items():
        #print("Downloading symbol: %s..." % sym, count)
        count = count + 1
        # Construct the message needed by IQFeed to retrieve data

        message = "HDT,%s,%s,20270101\n" % (sym, dte)
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
        #print(data)
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

    print("done")

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

def split_dict_into_chunks(dictionary, chunk_size):
    items = list(dictionary.items())
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    return [dict(chunk) for chunk in chunks]

if __name__ == '__main__':

    # record start time
    start = time.time()

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket portHI Hi


    param_dic = {
        "host": "localhost",
        "database": "markets_technicals",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    # get the list of all tickers from postgres table all_tickers
    date_tickers_default = get_all_tickers_defaultDates(con)



    # Changes begin here for three modes 1/only missing list 2/only RS list 3/all tickers

    #FIRST CHANGE
    #MISSED TICKERS
    #mis_list = ('AAPL','EIX')
    #date_tickers = get_dates_onlyRS_tickers(con, mis_list)

    # RS TICKERS
    #rs_list = get_rs_tickers(con)
    #date_tickers = get_dates_onlyRS_tickers(con, rs_list)


    #DEFAULT MODE - ALL TICKERS
    date_tickers = get_dates_all_tickers(con)

    #SECOND CHANGE
    #DEFAULT MODE - ALL TICKERS
    final_date_tickers_all = update_date_tickers_all(date_tickers_default,date_tickers)

    iq.launch_service()

    #FOR BOTH RS TICKERS AND MISSED TICKERS
    #final_date_tickers_some = update_date_tickers_some(date_tickers_default,date_tickers)

    list_dict = split_dict_into_chunks(final_date_tickers_all, 500)
    count=0
    for i in list_dict:
        count=count+1
        print(count)
        up_df = get_historical_data(i)
        copy_from_stringio(con, up_df, "usstockseod")


    #FOR MISSING LIST AND RS LIST, JUST BELOW ONE LINE IS ENOUGH AND COMMENT CHUNKS CODE AND LOOP CODE ABOVE
    #up_df = get_historical_data(final_date_tickers_some)

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3)/60000, "minutes")

    sys.exit(0)
