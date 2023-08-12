""" This script is to merge csvs and upload to a table """

import psycopg2
import sys
import pandas as pd
from io import StringIO

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

def copy_from_csv(conn, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    f = open(r"D:\data\db\key_indicators_population_delta3.csv", 'r')
    #buffer = StringIO()
    #dff.to_csv(buffer, index_label='id', header=False)

    #buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

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


def merge_csv(file1, file2):
    """
    This function is to merge two csv files into a dataframe
    """

    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df = pd.concat([df1,df2])
    df = df.drop_duplicates()
    df.to_csv(r"D:\data\db\all_tickers_D05142023.csv", index=False)

if __name__ == '__main__':
    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con=connect(param_dic)

    file = r"D:\data\db\key_indicators_population_delta3.csv"
    df = pd.read_csv(file)
    df = df.fillna(0)

    copy_from_stringio(con, df, "key_indicators_alltickers")


    # merge two csv files
    #merge_csv(file1, file2)

    #copy_from_csv(con,"key_indicators_alltickers")

    con.close()