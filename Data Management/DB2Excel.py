
""" This script is  to extract data from a database to df and to excel"""

import psycopg2
import pandas as pd
import sys
import datetime

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

def copy_from_db(conn,dte):
    cursor = conn.cursor()
    postgreSQL_select_Query = """select * from rs_industry_groups r where r.date = %s"""
    cursor.execute(postgreSQL_select_Query, [dte, ])
    close_prices = cursor.fetchall()
    c_f = pd.DataFrame(close_prices, columns=['date', 'industry', 'ticker', 'rs1', 'rs2', 'rs3', 'rs4', 'rs'])

    return c_f

if __name__ == '__main__':
    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    dateTimeObj = datetime.datetime.now()
    run_date = dateTimeObj - datetime.timedelta(days=3)
    run_date = run_date.strftime("%Y-%m-%d")

    con=connect(param_dic)
    path = r'C:\Users\uvdsa\Documents\Trading\Scripts\plurality-rawdata-d0603.csv'

    df=copy_from_db(con,run_date)

    df.to_csv(path)

    con.close()