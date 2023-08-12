import sys
import os
import psycopg2


def pg_connect():
    """ Connect to the PostgreSQL database server """
    try:
        param_dic = {
            "host": os.environ.get('POSTGRES_HOST'),
            "database": os.environ.get('POSTGRES_DB'),
            "user": os.environ.get('POSTGRES_USER'),
            "password": os.environ.get('POSTGRES_PASSWORD')
        }
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn