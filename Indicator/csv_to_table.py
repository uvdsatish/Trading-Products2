import psycopg2
import pandas as pd
from io import StringIO
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


def copy_from_stringio(conn, df, table):
    """
    Here we are going to save the dataframe in memory
    and use copy_from() to copy it to the database.
    """
    cursor = conn.cursor()
    # Create a buffer
    buffer = StringIO()

    # Write the dataframe to the buffer
    df.to_csv(buffer, index=False, header=False)

    # Move the cursor to the beginning of the buffer
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        problematic_line = buffer.getvalue().split('\n')[0]
        print("Problematic line: %s" % problematic_line)
        conn.rollback()
        cursor.close()
        return 1

    print("copy_from_stringio() done")
    cursor.close()






if __name__ == '__main__':

    # record start time
    start = time.time()

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    # Define the chunk size
    chunk_size = 1000000

    # Define the table name
    table_name = 'key_indicators_alltickers'

    # Path to your large CSV file
    csv_file_path = r"D:\data\db\key_indicators_population_allTickers.csv"

    i=1

    # Process the CSV file in chunks
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
        # Copy each chunk to the database
        print("processing chunk", i)
        copy_from_stringio(con, chunk, table_name)
        i=i+1

    print("Data has been successfully copied to the database.")

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")

    sys.exit(0)


