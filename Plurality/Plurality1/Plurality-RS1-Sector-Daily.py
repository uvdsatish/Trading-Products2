#This script is to calculate daily WAM RS values for Sector/Ticker combo

import pandas as pd
import psycopg2
import datetime
import sys
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


def get_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select sector, ticker from industry_groups"
    cursor.execute(postgreSQL_select_Query)
    ig_records = cursor.fetchall()

    df = pd.DataFrame(ig_records,
                      columns=['sector', 'ticker'] )
    return df


def get_rs_ticker(conn,tkr_list,dat):
    rs_dict = {}

    for ticker in tkr_list:
        df=get_rs_values_ticker(conn, ticker,dat)
        RS1 = df.loc[df['ticker']==ticker,'rs1'].values[0]
        RS2 = df.loc[df['ticker'] == ticker, 'rs2'].values[0]
        RS3 = df.loc[df['ticker'] == ticker, 'rs3'].values[0]
        RS4 = df.loc[df['ticker'] == ticker, 'rs4'].values[0]
        RS = df.loc[df['ticker'] == ticker, 'rs'].values[0]
        rs_dict[ticker]=[RS1,RS2,RS3,RS4,RS]

    return rs_dict



def get_rs_values_ticker(connn, ticker,datt):
    cursor = connn.cursor()
    postgreSQL_select_Query = """select date,ticker,rs1,rs2,rs3,rs4,rs from rs_industry_groups r where r.ticker = %s ;"""
    cursor.execute(postgreSQL_select_Query, [ticker, ])
    rs_values = cursor.fetchall()
    rs_df = pd.DataFrame(rs_values, columns=['date', 'ticker', 'rs1', 'rs2','rs3','rs4','rs'])
    dattt = datetime.datetime.strptime(datt,'%Y-%m-%d').date()
    rs_df = rs_df.loc[rs_df['date']==dattt]

    return rs_df




def update_RS(ddf,RS_dict,dateee):
    #get the data frame: timestamp, date, sector, ticker, RS1, RS2, RS3, RS4, RS
    rs_df = pd.DataFrame()
    rs_df['date'] = pd.Series([dateee for x in range(len(ddf.index))])
    rs_df['sector'] = ddf['sector']
    rs_df['ticker'] = ddf['ticker']

    for t,rs_list in RS_dict.items():
        rs_df.loc[rs_df['ticker']== t,'rs1'] = rs_list[0]
        rs_df.loc[rs_df['ticker'] == t,'rs2'] = rs_list[1]
        rs_df.loc[rs_df['ticker'] == t,'rs3'] = rs_list[2]
        rs_df.loc[rs_df['ticker'] == t,'rs4'] = rs_list[3]
        rs_df.loc[rs_df['ticker'] == t,'rs'] = rs_list[4]

    return rs_df


def update_sectors_rs(conn, dff, table):
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



if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    df = get_tickers(con)

    # RS for a particular date - default today
    dateTimeObj = datetime.datetime.now()
    datee= dateTimeObj-datetime.timedelta(days=1)
    datee = datee.strftime("%Y-%m-%d")
    print(datee)


    ticker_list = list(df.ticker.unique())

    rs_dict=get_rs_ticker(con,ticker_list,datee)

    rs_d=update_RS(df,rs_dict,datee)

    rs_d = rs_d[["date","sector","ticker","rs1","rs2","rs3","rs4","rs"]]

    update_sectors_rs(con,rs_d,"rs_sectors")

    con.close()



