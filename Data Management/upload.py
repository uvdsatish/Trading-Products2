""" This script is upload historical data from a csv file"""

import psycopg2
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

def copy_from_csv(conn, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    f = open(r'C:\Users\uvdsa\Documents\Trading\Scripts\plurality-IG_historical370-test.csv', 'r')
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


if __name__ == '__main__':
    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con=connect(param_dic)

    copy_from_csv(con,"rs_industry_groups_history")

    con.close()