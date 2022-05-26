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


def get_sectors_data(sects,a_df):
    s_df = a_df.sort_values(by=['sector', 'marketcap'], ascending=[True, False], na_position='last')
    cols = ['sector', 'ticker', 'name', 'industry', 'volume', 'marketcap']
    s_df = s_df[cols]
    s_df = s_df.loc[s_df['sector'] != ""]

    s_tot_df = pd.DataFrame()

    for s in sects:
        t_df = s_df.loc[s_df['sector'] == s]
        t_df = t_df.loc[t_df['volume']>=100]
        s_tot_df = pd.concat([s_tot_df, t_df.head(20)])

    return s_tot_df


def get_ig_data(grps, a_df):
    ig_df = a_df.sort_values(by=['industry', 'marketcap'], ascending=[True, False], na_position='last')
    cols = ['industry', 'ticker', 'name', 'sector', 'volume', 'marketcap']
    ig_df = ig_df[cols]

    ig_tot_df = pd.DataFrame()

    for g in grps:
        t_df = ig_df.loc[ig_df['industry'] == g]
        t_df = t_df.loc[t_df['volume']>=100]
        ig_tot_df = pd.concat([ig_tot_df, t_df.head(10)])

    return ig_tot_df


def create_tables(conn):
    commands = (
        """
        DROP TABLE IF EXISTS industry_groups;
        """
       """
        DROP TABLE IF EXISTS sectors;
        """ 
        """
        CREATE TABLE industry_groups (
            industry VARCHAR(255) NOT NULL,
            ticker VARCHAR(255) NOT NULL,
            name VARCHAR(255),
            sector VARCHAR(255),
            volume INTEGER,
            marketcap FLOAT,
            PRIMARY KEY (industry, ticker)
            )          
        """,
        """
               CREATE TABLE sectors (
                   sector VARCHAR(255) NOT NULL,
                   ticker VARCHAR(255) NOT NULL,
                   name VARCHAR(255),  
                   industry VARCHAR(255),
                   volume INTEGER,
                   marketcap FLOAT,
                   PRIMARY KEY (sector, ticker) 
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
    all_df = get_sectors_ig(con)

    sectors = list(all_df.sector.unique())
    groups = list(all_df.industry.unique())

    igroups_df = get_ig_data(groups,all_df)
    sec_df = get_sectors_data(sectors,all_df)

    create_tables(con)
    update_tables(con,igroups_df,sec_df)

    con.close()



