import pandas as pd
import psycopg2
from io import StringIO
import datetime
import sys
import socket
import time
import iqfeedTest as iq


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def get_tickers_from_iblkupall(conn):
    """ Fetch tickers from iblkupall if usstockseod is empty """
    cursor = conn.cursor()
    postgreSQL_select_Query = "SELECT ticker FROM iblkupall"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records, columns=['ticker'])
    # Set the default start date to 1950-01-01 and format it as YYYYMMDD
    df['start_date'] = '19500101'
    # Set the end date to the current date
    df['end_date'] = pd.to_datetime('today').strftime('%Y%m%d')

    return df


def check_if_usstockseod_is_empty(conn):
    """ Check if the usstockseod table is empty """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM usstockseod")
    count = cursor.fetchone()[0]
    return count == 0



import time

def get_historical_data(dct_tickers, max_retries=5, retry_delay=10):
    """ Fetch historical data from IQFeed for the given tickers with retry on failure """
    fdf = pd.DataFrame()
    columns = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
    excp = []
    count = 1
    verr = []

    for sym, dte_range in dct_tickers.items():
        start_date, end_date = dte_range
        count += 1

        message = f"HDT,{sym},{start_date},{end_date}\n"
        message = bytes(message, encoding='utf-8')

        retries = 0
        while retries < max_retries:
            try:
                # Connect to IQFeed to send and receive data
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                sock.sendall(message)
                data = read_historical_data_socket(sock)
                sock.close()
                break  # Successful connection, exit the retry loop
            except ConnectionRefusedError as e:
                retries += 1
                print(f"Connection refused for {sym}. Retrying {retries}/{max_retries}...")
                time.sleep(retry_delay)  # Wait before retrying

        if retries == max_retries:
            print(f"Failed to connect after {max_retries} attempts for {sym}. Skipping this ticker.")
            excp.append(sym)
            continue

        if "!NO_DATA!" in data:
            print(f"No data for {sym}")
            excp.append(sym)
            continue

        data = str(data).replace("\r", "").replace(",\n", "\n")[:-1]
        dd_ls1 = data.split('\n')
        dd_ls2 = [i.split(',') for i in dd_ls1]
        try:
            ddf = pd.DataFrame(dd_ls2, columns=columns)
        except ValueError:
            print(f"Connection error for {sym}")
            verr.append(sym)
            continue
        else:
            ddf.insert(0, 'Ticker', sym)
            fdf = pd.concat([fdf, ddf], ignore_index=True)
            del ddf

    print("No data for these tickers:", excp)
    print("Connection errors for these tickers:", verr)
    return fdf

def read_historical_data_socket(sock, recv_buffer=4096):
    """ Read data from IQFeed socket """
    buffer = ""
    while True:
        data = str(sock.recv(recv_buffer), encoding='utf-8')
        buffer += data
        if "!ENDMSG!" in buffer:
            break
    return buffer[:-12]


def copy_from_stringio(conn, dff, table):
    """ Copy data from pandas DataFrame to PostgreSQL """
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
    print("Data copied to PostgreSQL successfully")
    cursor.close()


def split_dict_into_chunks(dictionary, chunk_size):
    """ Split dictionary into chunks for efficient processing """
    items = list(dictionary.items())
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    return [dict(chunk) for chunk in chunks]

def get_remaining_tickers_from_iblkupall(conn):
    """
    Fetch tickers from iblkupall that are not present in usstockseod
    and use the default date range (1950-01-01 to current date).
    """
    cursor = conn.cursor()
    query = """
    SELECT ticker
    FROM iblkupall
    WHERE ticker NOT IN (SELECT DISTINCT ticker FROM usstockseod)
    """
    cursor.execute(query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records, columns=['ticker'])
    # Set the default start date to 1950-01-01 and format it as YYYYMMDD
    df['start_date'] = '19500101'
    # Set the end date to the current date
    df['end_date'] = pd.to_datetime('today').strftime('%Y%m%d')

    return df


if __name__ == '__main__':

    # Record start time
    start = time.time()

    host = "127.0.0.1"  # Localhost
    port = 9100  # IQFeed Historical data socket port

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    # Check if the usstockseod table is empty
    if check_if_usstockseod_is_empty(con):
        # If usstockseod is empty, get tickers from iblkupall with default date range
        tickers_df = get_tickers_from_iblkupall(con)
    else:
        # If usstockseod has data, get tickers from iblkupall that are not in usstockseod
        print("usstockseod is not empty. Getting remaining tickers from iblkupall...")
        tickers_df = get_remaining_tickers_from_iblkupall(conn=con)

    # Print the number of tickers being processed
    ticker_count = len(tickers_df)
    print(f"Number of tickers being processed: {ticker_count}")

    # Create dictionary {ticker: (start_date, end_date)}
    tickers_dict = tickers_df.set_index('ticker')[['start_date', 'end_date']].to_dict(orient='index')

    # Split the dictionary into chunks of 500 tickers
    ticker_chunks = split_dict_into_chunks(tickers_dict, 500)

    iq.launch_service()

    count = 0
    for ticker_chunk in ticker_chunks:
        count += 1
        print(f"Processing chunk {count}...")
        up_df = get_historical_data(ticker_chunk)
        copy_from_stringio(con, up_df, "usstockseod")
        # Pause to avoid overwhelming the system or IQFeed rate limits
        time.sleep(10)

    con.close()

    # Record end time
    end = time.time()

    print("The time of execution of the program is :", ((end - start) * 10 ** 3) / 60000, "minutes")
    sys.exit(0)
