# This script is used to get historical data for a list of symbols from iq feed and upload data from dataframe to DB
import pandas as pd
import socket
import sys
import psycopg2
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, BigInteger, Float, DateTime
from sqlalchemy import create_engine
from io import StringIO


Base = declarative_base()

class usstockseod(Base):
    __tablename__ = 'usstockseod'
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    timestamp = Column(DateTime)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    openinterest = Column(BigInteger)

    def __repr__(self):
        return "<Book(ticker ={},timestamp='{}', high='{}', low={}, open={}, close={}, volume={}, openinterest={})>" \
            .format(self.ticker, self.timestamp, self.high, self.low, self.open, self.close, self.volume, self.openinterest)

def recreate_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)



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


def get_tickers_list(conn):
    tickers_select_query = "select ticker from iblkupall"
    cursor = conn.cursor()
    cursor.execute(tickers_select_query)
    lst_tickers = cursor.fetchall()
    l_t = []
    for i in lst_tickers:
        l_t.append(i[0])
    return l_t
    conn.close()


def get_historical_data(l_tickers):
    fdf = pd.DataFrame()
    columns = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
    excp=[]
    count=1
    verr=[]

    for sym in l_tickers:
        print("Downloading symbol: %s..." % sym, count)
        count = count+1
        # Construct the message needed by IQFeed to retrieve data

        message = "HDT,%s,20220521,20250101\n" % sym
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
            print("no data for %s "% sym, count)
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

    DATABASE_URI = 'postgresql+psycopg2://postgres:root@localhost:5432/Plurality'
    engine = create_engine(DATABASE_URI)
    Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)

    # Connection parameters
    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }
    con = connect(param_dic)

    #list_tickers = get_tickers_list(con)
    list_tickers =['EWBC','ZNGA','NTGR','NAD','MPAY', 'KMT', 'IQV', 'PRTY', 'UTME','EGLE']
    df = get_historical_data(list_tickers)

    copy_from_stringio(con, df, "usstockseod")
    con.close()
