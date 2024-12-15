#This script is to calculate daily WAM RS values for industry/ticker

import pandas as pd
import psycopg2
import datetime
from dateutil.relativedelta import relativedelta
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


def get_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select industry, ticker from industry_groups"
    cursor.execute(postgreSQL_select_Query)
    ig_records = cursor.fetchall()

    df = pd.DataFrame(ig_records,
                      columns=['industry', 'ticker'] )
    return df


def get_rs_ticker(cl_df,tkr_list,dat):
    rs_dict ={}
    excp =[]
    count = 0
    tot = len(tkr_list)

    for ticker in tkr_list:
        count = count + 1
        #print("calculating RS for ticker %s %s/%s" % (ticker, count,  tot))
        df=cl_df.loc[cl_df['ticker'] == ticker]
        #df= df.sort_values(by='date', ascending=False, inplace=True)
        if len(df.index) == 0:
            rs_dict[ticker]=[None, None, None, None, None]
            excp.append(ticker)
        else:
            RS1 = calculate_RS(df, dat, 3)
            RS2 = calculate_RS(df, dat, 6)
            RS3 = calculate_RS(df, dat, 9)
            RS4 = calculate_RS(df, dat, 12)
            if (RS1 == None or RS2==None or RS3==None or RS4==None):
                RS=None
                excp.append(ticker)
            else:
                RS = round(0.4*RS1+0.2*RS2+0.2*RS3+0.2*RS4,0)
            rs_dict[ticker]=[RS1,RS2,RS3,RS4,RS]

    print(excp)

    return rs_dict


def get_rs_ticker_old(conn,tkr_list,dat):
    rs_dict ={}
    excp =[]
    count = 0
    tot = len(tkr_list)

    for ticker in tkr_list:
        count = count + 1
        #print("calculating RS for ticker %s %s/%s" % (ticker, count,  tot))
        df=get_close_ticker_old(conn, ticker)
        if len(df.index) == 0:
            rs_dict[ticker]=[None, None, None, None, None]
            excp.append(ticker)
        else:
            RS1 = calculate_RS(df, dat, 3)
            RS2 = calculate_RS(df, dat, 6)
            RS3 = calculate_RS(df, dat, 9)
            RS4 = calculate_RS(df, dat, 12)
            if (RS1 == None or RS2==None or RS3==None or RS4==None):
                RS=None
                excp.append(ticker)
            else:
                RS = round(0.4*RS1+0.2*RS2+0.2*RS3+0.2*RS4,0)
            rs_dict[ticker]=[RS1,RS2,RS3,RS4,RS]

    print(excp)

    return rs_dict


def calculate_RS(ddf,edate,m):
    bdate = edate - relativedelta(months=m)
    edate = edate.strftime('%Y-%m-%d')
    bdate = bdate.strftime('%Y-%m-%d')

    sub_df = ddf[(ddf['date'] >= bdate) & (ddf['date'] <= edate)]

    if len(sub_df.index) == 0:
        return None
    else:
        close_price =sub_df[sub_df['date']==edate]['close']
        lowest_price =sub_df['close'].min()
        highest_price =sub_df['close'].max()
        if (len(close_price.index) == 0) or (highest_price == lowest_price):
            return None
        else:
            RS = round(((close_price.iloc[0] - lowest_price) / (highest_price - lowest_price)) * 100, 0)
            return RS



def get_close_ticker_all(connn):
    cursor = connn.cursor()
    postgreSQL_select_Query = """select ticker, timestamp, close from usstockseod_sinceapril2023_view """
    cursor.execute(postgreSQL_select_Query)
    close_prices = cursor.fetchall()
    c_f = pd.DataFrame(close_prices, columns=['ticker', 'timestamp', 'close'])
    c_f['date'] = pd.to_datetime(c_f['timestamp'])
    c_f['date'] = c_f['date'].dt.strftime('%Y-%m-%d')

    return c_f


def get_close_ticker_old(connn, ticker):
    cursor = connn.cursor()
    postgreSQL_select_Query = """select ticker, timestamp, close from usstockseod u where u.ticker = %s order by timestamp desc limit 400"""
    cursor.execute(postgreSQL_select_Query, [ticker, ])
    close_prices = cursor.fetchall()
    c_f = pd.DataFrame(close_prices, columns=['ticker', 'timestamp', 'close'])
    c_f['date'] = pd.to_datetime(c_f['timestamp'])
    c_f['date'] = c_f['date'].dt.strftime('%Y-%m-%d')
    c_f.sort_values(by='date', ascending=False, inplace=True)

    return c_f


def update_RS(ddf,RS_dict,dateee,conn):
    #get the data frame: timestamp, date, industry group, ticker, RS1, RS2, RS3, RS4, RS
    rs_df = pd.DataFrame()
    rs_df['timestamp'] = pd.Series([datetime.datetime.timestamp(datee)for x in range(len(ddf.index))])
    rs_df['date'] = pd.Series([dateee.strftime('%Y-%m-%d')for x in range(len(ddf.index))])
    rs_df['industry'] = ddf['industry']
    rs_df['ticker'] = ddf['ticker']

    for t,rs_list in RS_dict.items():
        rs_df.loc[rs_df['ticker']== t,'RS1'] = rs_list[0]
        rs_df.loc[rs_df['ticker'] == t,'RS2'] = rs_list[1]
        rs_df.loc[rs_df['ticker'] == t,'RS3'] = rs_list[2]
        rs_df.loc[rs_df['ticker'] == t,'RS4'] = rs_list[3]
        rs_df.loc[rs_df['ticker'] == t,'RS'] = rs_list[4]

    return rs_df


def update_ind_groups(conn, dff, table):
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
    # record start time
    start = time.time()

    param_dic = {
        "host": "localhost",
        "database": "markets_technicals",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    df = get_tickers(con)

    # RS for a particular date - default today
    dateTimeObj = datetime.datetime.now()
    datee= dateTimeObj-datetime.timedelta(days=0)
    print(datee)

    ticker_list = list(df.ticker.unique())
    df_close = get_close_ticker_all(con)

    rs_dict=get_rs_ticker(df_close,ticker_list,datee)

    rs_d=update_RS(df,rs_dict,datee,con)

    rs_d = rs_d[["date","industry","ticker","RS1","RS2","RS3","RS4","RS"]]

    update_ind_groups(con,rs_d,"rs_industry_groups")
    update_ind_groups(con, rs_d, "rs_industry_groups_history")

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")

    sys.exit(0)

