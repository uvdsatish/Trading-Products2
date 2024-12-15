import psycopg2
import sys
import time

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    print("Connection successful")
    return conn


def remove_duplicates(conn):
    """ Execute the SQL to remove duplicates """
    try:
        cursor = conn.cursor()

        # Define the SQL query to remove duplicates
        sql_query = """
        WITH duplicates AS (
            SELECT ctid, 
                   ROW_NUMBER() OVER (PARTITION BY ticker, timestamp::DATE ORDER BY ctid) AS rnum
            FROM usstockseod
        )
        DELETE FROM usstockseod
        WHERE ctid IN (
            SELECT ctid FROM duplicates WHERE rnum > 1
        )
        RETURNING ctid;;
        """

        # Execute the query
        cursor.execute(sql_query)

        # Fetch the count of deleted rows
        deleted_rows = cursor.fetchall()
        num_deleted = len(deleted_rows)

        # Commit the changes
        conn.commit()

        print(f"Duplicates removed successfully. {num_deleted} rows were deleted.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()  # Rollback in case of error
        sys.exit(1)

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

    remove_duplicates(con)

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")

    sys.exit(0)