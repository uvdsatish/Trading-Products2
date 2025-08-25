# Delete tickers or corresponding rows from a table

import psycopg2
import sys
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


def delete_tickers(conn, ticker):
    try:
        cursor = conn.cursor()

        postgreSQL_select_Query = "DELETE FROM industry_groups WHERE ticker = %(tlist)s"

        query_params = {'tlist': ticker}

        cursor.execute(postgreSQL_select_Query, query_params)

        # Get the number of records deleted
        records_deleted = cursor.rowcount

        # Commit the transaction to apply changes
        conn.commit()

        if records_deleted == 1:
            print("1 record deleted")
        else:
            print(f"{records_deleted} records deleted")

    except Exception as e:
        # Handle exceptions, such as database errors
        conn.rollback()
        print("Error:", e)
    finally:
        cursor.close()

if __name__ == '__main__':

    # record start time
    start = time.time()

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port


    param_dic = {
        "host": "localhost",
        "database": "markets_technicals",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    # Put the ticker
    ticker = 'GOGL'

    delete_tickers(con, ticker)

    con.close()

    sys.exit(0)