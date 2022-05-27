# This script creates sector and groups tables with top market cap stocks

import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, BigInteger, Float, DateTime
from sqlalchemy import create_engine
from io import StringIO
import datetime
import sys
import socket

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


def get_sectors_ig(conn):
    cursor = con.cursor()
    postgreSQL_select_Query = "select * from iblkupall"
    cursor.execute(postgreSQL_select_Query)
    sector_records = cursor.fetchall()

    df = pd.DataFrame(sector_records,
                      columns=['industry', 'ticker', 'name', 'sector', 'volume', 'marketcap'])

    cols = ['ticker', 'name', 'industry', 'sector', 'volume', 'marketcap']

    mod_df = df[cols]

    return mod_df



def create_tables(conn):
    commands = (
        """
        DROP TABLE IF EXISTS rs_industry_groups;
        """
       """
        DROP TABLE IF EXISTS rs_industry_groups_plurality;
        """ 
        """
        CREATE TABLE rs_industry_groups (
            date DATE NOT NULL,
            industry VARCHAR(255),
            ticker VARCHAR(255),
            RS1 FLOAT,
            RS2 FLOAT,
            RS3 FLOAT,
            RS4 FLOAT,
            RS FLOAT,
            PRIMARY KEY (date, industry, ticker)
            )          
        """,
        """
        CREATE TABLE rs_industry_groups_plurality(
            date DATE NOT NULL,
            industry VARCHAR(255),
            P5090 VARCHAR DEFAULT 'N',
            P4080 VARCHAR DEFAULT 'N',
            P5010 VARCHAR DEFAULT 'N',
            P4020 VARCHAR DEFAULT 'N',
            c80 INTEGER,
            c90 INTEGER,
            c10 INTEGER,
            c20 INTEGER,
            totC INTEGER,
            AvgRS FLOAT,
            PRIMARY KEY (date, industry) 
            )         
               """
    )

    try:
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


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


def update_tables(con, ig_df, s_df):
    copy_from_stringio(con,ig_df,"industry_groups")
    copy_from_stringio(con, s_df, "sectors")

if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    create_tables(con)
    #update_tables(con,igroups_df,sec_df)

    con.close()



