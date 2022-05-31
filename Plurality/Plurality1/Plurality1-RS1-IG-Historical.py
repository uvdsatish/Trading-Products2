#This script is to calculate daily WAM RS values for industry/ticker historically for certain number of days
# how many calendar days to run for a group of ticker/portfolio combo - get this as input
# get all the trading dates - say for AAPL and use those dates for all the tickers or use the logic you implied
# now you've dates in a list and tickers in a list
# get all us stockseod data for all tickers and all dates in a dataframe
# for a date calculate the RS for all tickers
# repeat for all dates and then add the dataframe to the table
# what about IPOs, and missing tickers for certain dates?

import pandas as pd
import psycopg2
import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
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

def get_rs_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker from industry_groups"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker'])
    RS_list = tuple(df.ticker.unique())
    return RS_list


def get_industry_tickers(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select industry, ticker from industry_groups"
    cursor.execute(postgreSQL_select_Query)
    ig_records = cursor.fetchall()

    df = pd.DataFrame(ig_records,
                      columns=['industry', 'ticker'] )
    return df


def get_rs_ticker(mdf,tkr_list,dat):
    rs_dict ={}
    excp =[]
    count = 0
    tot = len(tkr_list)

    for ticker in tkr_list:
        count = count + 1
        print("calculating RS for ticker %s %s/%s" % (ticker, count,  tot))
        df = mdf.loc[mdf['ticker']==ticker]
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


def  get_allclose_alltickers(conn,ticker_list):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker, timestamp, close from usstockseod where ticker in %s" % (ticker_list,)
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    c_f = pd.DataFrame(stock_records, columns=['ticker', 'timestamp', 'close'])
    c_f['date'] = pd.to_datetime(c_f['timestamp'])
    c_f['date'] = c_f['date'].dt.strftime('%Y-%m-%d')
    c_f.sort_values(by='date', ascending=False, inplace=True)
    c_f['mod_ts'] = pd.to_datetime(c_f['date'], format='%Y-%m-%d')

    return c_f



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



def update_RS(ddf,RS_dict,dateee):
    #get the data frame: timestamp, date, industry group, ticker, RS1, RS2, RS3, RS4, RS
    rs_df = pd.DataFrame()
    rs_df['timestamp'] = pd.Series([datetime.datetime.timestamp(dateee)for x in range(len(ddf.index))])
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

def get_trading_dates(ncd,md_df):
    delta_days_from_current_date = ncd

    end_date = datetime.datetime.now()
    begin_date = end_date- datetime.timedelta(days=delta_days_from_current_date)

    app_df = massive_df.loc[massive_df['ticker'] == 'AAPL']

    tmp_str = app_df.loc[(app_df['mod_ts'] >= begin_date) & (app_df['mod_ts'] <= end_date), 'date']
    dates_str = tmp_str.to_list()

    return dates_str


def get_rs_alldates_allRStickers(massive_df,it_df,dates_str,ticker_list):
    big_df = pd.DataFrame()
    for dte in dates_str:
        print("printing for the date %s" % dte)
        dte_ts = datetime.datetime.strptime(dte, "%Y-%m-%d")
        date_dct = get_rs_ticker(massive_df,ticker_list,dte_ts)
        rs_df = update_RS(it_df, date_dct, dte_ts)
        rs_df = rs_df[["date", "industry", "ticker", "RS1", "RS2", "RS3", "RS4", "RS"]]
        big_df = pd.concat([big_df, rs_df])

    return big_df


if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    ticker_list = get_rs_tickers(con)

    massive_df = get_allclose_alltickers(con, ticker_list)

    # RS for a particular date - default today
    NumberofCalendarDays =370

    dates_str= get_trading_dates(NumberofCalendarDays,massive_df)

    it_df = get_industry_tickers(con)
    all_df = get_rs_alldates_allRStickers(massive_df,it_df,dates_str,ticker_list)

    all_df.to_csv(r"C:\Users\uvdsa\Documents\Trading\Scripts\plurality-IG_historical370_final.csv", index=False, header=False)

    con.close()



