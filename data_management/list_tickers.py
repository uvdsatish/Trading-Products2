import pandas as pd
import psycopg2
from io import StringIO
import datetime
import sys


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


def get_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select distinct ticker from usstockseod "
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker']).sort_values(by=['ticker'])
    t_list = list(df.ticker)

    return t_list


if __name__ == '__main__':

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket portHI Hi


    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    t_list = get_tickers(con)

    print(len(t_list))

    path_str = r"D:\data\db\list_tickers1.csv"

    # list to dataframe

    df = pd.DataFrame(t_list)
    df.to_csv(path_str)


