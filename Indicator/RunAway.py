# Read an excel file with a list of tickers, go to the key indicators table, get the required columns, and populate the output excel

import pandas as pd
import psycopg2
import sys
import time
import datetime


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

def read_tickers(excel_file):
    # read excel file in to pandas data frame
    excel_df = pd.read_excel(excel_file)
    long_list  = excel_df['Ticker_Long'].dropna().unique().tolist()
    short_list  = excel_df['Ticker_Short'].dropna().unique().tolist()


    return long_list, short_list

def get_tickers_data(conn, tickers_list, datee):
    # get columns from db table

    cursor = conn.cursor()

    postgreSQL_select_Query = "select * from key_indicators_alltickers_sinceapril2022_view where ticker in %(tlist)s and date = %(dobject)s"

    query_params = {'tlist': tuple(tickers_list), 'dobject': datee}

    cursor.execute(postgreSQL_select_Query, query_params)

    df_records = cursor.fetchall()


    #cursor = conn.cursor()
    #placeholders = ', '.join(['%s' for _ in tickers_list])
    #postgreSQL_select_Query = f"SELECT * FROM key_indicators_alltickers_sinceapril2022_view WHERE ticker IN ({placeholders}) AND date = %s"
    #cursor.execute(postgreSQL_select_Query, tickers_list + [datee])


    #df_records = cursor.fetchall()

    df = pd.DataFrame(df_records, columns=[desc[0] for desc in cursor.description])

    df= df.round(2)

    df['dayRange'] = df['high'] -df['low']
    df['atr_fade_25per'] = df['atr14']*0.25


    return df
def update_excel(df,excel_file):
    df.to_excel(excel_file)




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

    dateTimeObj = datetime.datetime.now()
    datee = dateTimeObj - datetime.timedelta(days=0)
    date_object = datee.date()

    #excel_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily_Task\daily_work\Daily_Tech_Reading\Weekly_list.xlsx"
    excel_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily_Task\daily_work\Daily_Tech_Reading\Runaway_active.xlsx"
    long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily_Task\daily_work\Daily_Tech_Reading\Runaway-long_file.xlsx"
    short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily_Task\daily_work\Daily_Tech_Reading\Runaway-short_file.xlsx"

    long_list, short_list = read_tickers(excel_file)

    long_df = get_tickers_data(con,long_list, date_object)
    short_df = get_tickers_data(con,short_list, date_object)

    columns_to_select = ['ticker','date','high','low','open','close', 'volume', 'vma30','tradingrange', 'dayRange', 'atr14', 'atr_fade_25per','sma10', 'ema20', 'sma50', 'w52high', 'w52low', 'nr7', 'tr3', 'exhaust_trade_bull', 'exhaust_trade_bear', 'runaway_up_521','runaway_down_521', 'runaway_up_1030', 'runaway_down_1030', 'runaway_up_0205', 'runaway_down_0205']
    long_subset_df = long_df[columns_to_select]
    short_subset_df = short_df[columns_to_select]

    long_subset_df = long_subset_df.copy()
    long_subset_df['sma10_alert'] = long_subset_df['sma10'] + long_subset_df['atr_fade_25per']
    long_subset_df['ema20_alert'] = long_subset_df['ema20'] + long_subset_df['atr_fade_25per']
    long_subset_df['new_high'] = (long_subset_df['high'] >= long_subset_df['w52high']-long_subset_df['atr_fade_25per'])
    long_subset_df['Net0521'] = long_subset_df['runaway_up_521'] - long_subset_df['runaway_down_521']
    long_subset_df['Net1030'] = long_subset_df['runaway_up_1030'] - long_subset_df['runaway_down_1030']
    long_subset_df['Net0205'] = long_subset_df['runaway_up_0205'] - long_subset_df['runaway_down_0205']

    short_subset_df = short_subset_df.copy()
    short_subset_df['sma10_alert'] = short_subset_df['sma10'] - short_subset_df['atr_fade_25per']
    short_subset_df['ema20_alert'] = short_subset_df['ema20'] - short_subset_df['atr_fade_25per']
    short_subset_df['Net0521'] = short_subset_df['runaway_up_521'] - short_subset_df['runaway_down_521']
    short_subset_df['Net1030'] = short_subset_df['runaway_up_1030'] - short_subset_df['runaway_down_1030']
    short_subset_df['Net0205'] = short_subset_df['runaway_up_0205'] - short_subset_df['runaway_down_0205']
    short_subset_df['new_low'] = (
                short_subset_df['low'] <= short_subset_df['w52low'] + short_subset_df['atr_fade_25per'])

    long_subset_df = long_subset_df.round(2)
    short_subset_df = short_subset_df.round(2)

    long_subset_df = long_subset_df.sort_values(by='Net1030', ascending=False)
    short_subset_df = short_subset_df.sort_values(by='Net1030', ascending=False)

    update_excel(long_subset_df, long_file)
    update_excel(short_subset_df, short_file)

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")
