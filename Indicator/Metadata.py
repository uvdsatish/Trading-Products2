# Read an excel file with a list of tickers and dates and other data such as ATR, P&L for closed trades 3 months prior (not counting this month), go to the prices_table and get the prices for the right date

import pandas as pd
import psycopg2
import sys
import time
import numpy as np
import datetime
#from datetime import datetime, timedelta


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

def get_valid_dates(con):
    # get valid trading dates
    cursor = con.cursor()
    select_query = "select timestamp from valid_trading_dates"
    cursor.execute(select_query)
    valid_dates = cursor.fetchall()


    df = pd.DataFrame(valid_dates, columns=['timestamp'])

    # Convert the 'timestamp' column to a datetime object
    df['date'] = pd.to_datetime(df['timestamp'])

    # Format the 'date' column as 'yyyy-mm-dd'
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    # Sort the DataFrame based on the 'date' column in ascending order
    df = df.sort_values(by='date', ascending=True)

    df = df.reset_index(drop=True)

    return df




def read_input_data(excel_file):
    # read excel file in to pandas data frame- which has tradeID, Pyramid#, status, ticker, direction, entry_date, final_exit date,atr, entry price, ops level, target 1, target 2, final exit price, trade duration, T-$, T-R
    excel_df = pd.read_excel(excel_file)

    selected_columns = ['S.No', 'System', 'TradeId','Pyramid#','Status','Ticker', 'Direction', 'Entry Date', 'FinalExitDate', 'ATR', 'R/R', 'Entry Price', 'Options', 'OPS Level',  'Target-1', 'Target-2', 'FinalExitPrice', 'TradeDuration', 'T-P&L', 'T-P&L( R )']

    excel_df = excel_df[selected_columns]

    condition = (excel_df['Status'] == "Closed") & (excel_df['FinalExitDate'] <= "07-31-2023")

    filtered_df = excel_df[condition]

    delta = filtered_df['FinalExitDate'] - filtered_df['Entry Date']
    delta = delta.apply(pd.Timedelta, unit='D')


    filtered_df['exit_date2'] = filtered_df['FinalExitDate'] + delta


    return filtered_df

def populate_required_dates(in_df,vd_df):
    out_df = in_df
    res_series = in_df.apply(get_dates_data, axis=1, result_type='expand', args=(vd_df,))

    out_df = pd.concat([out_df, res_series], axis=1)

    return out_df

def get_dates_data(row, vd_df):

    date1 = row['Entry Date'].date()
    d_one = get_specific_date(vd_df, date1, 1)
    d_two = get_specific_date(vd_df, date1, 2)
    d_three = get_specific_date(vd_df, date1, 3)
    d_week1 = get_specific_date(vd_df, date1, 5)
    d_week2 = get_specific_date(vd_df, date1, 10)
    d_week3 = get_specific_date(vd_df, date1, 15)
    d_month1 = get_specific_date(vd_df, date1, 21)
    d_month2 = get_specific_date(vd_df, date1, 42)
    d_quarter = get_specific_date(vd_df, date1, 63)

    date2 = row['FinalExitDate'].date()
    de_one = get_specific_date(vd_df, date2, 1)
    de_two = get_specific_date(vd_df, date2, 2)
    de_three = get_specific_date(vd_df, date2, 3)
    de_week1 = get_specific_date(vd_df, date2, 5)
    de_week2 = get_specific_date(vd_df, date2, 10)
    de_week3 = get_specific_date(vd_df, date2, 15)
    de_month1 = get_specific_date(vd_df, date2, 21)
    de_month2 = get_specific_date(vd_df, date2, 42)
    de_quarter = get_specific_date(vd_df, date2, 63)
    dates_list = [d_one, d_two, d_three, d_week1, d_week2, d_week3, d_month1, d_month2, d_quarter, de_one, de_two, de_three, de_week1, de_week2, de_week3, de_month1, de_month2, de_quarter]
    index_names = ["d_one", "d_two", "d_three", "d_week1", "d_week2", "d_week3", "d_month1", "d_month2", "d_quarter", "de_one", "de_two", "de_three", "de_week1", "de_week2", "de_week3", "de_month1", "de_month2", "de_quarter"]

    dates_series = pd.Series(dates_list,index=index_names)

    return dates_series
def get_specific_date(df, specific_date, period):


    # Find the index of the specific_date in the DataFrame
    date_index = df[df['date'] == specific_date.strftime('%Y-%m-%d')].index[0]

    # Check if there's a prior date
    if date_index > 0:
        specific_date = df.iloc[date_index + period]['date']
        return specific_date
    else:
        return None  # No prior date available

def get_prices_df(con, df):
    # Get unique ticker and date combinations
    tickers = df['Ticker'].unique()
    dates = np.unique(df.loc[:,'d_one':'de_quarter'].values.flatten())

    # Construct query to get all prices data in one query
    query = """
  SELECT ticker, date, close 
  FROM key_indicators_alltickers_sinceapril2022_view
  WHERE ticker IN %(tkrs)s AND date IN %(dts)s
  """

    query_params = {
        'tkrs': tuple(tickers),
        'dts': tuple(dates)
    }

    # Execute query
    prices_df = pd.read_sql(query, con, params=query_params)

    prices_df['date'] = prices_df['date'].astype(str)



    return prices_df


def get_tickers_df(con, df):
    # Get unique tickers all prices
    tickers = df['Ticker'].unique()

    print("getting all prices for tickers")
    # Construct query to get all prices data in one query
    query = """
  SELECT ticker, date, open, high, low, close 
  FROM key_indicators_alltickers_sinceapril2022_view
  WHERE ticker IN %(tkrs)s
  """

    query_params = {
        'tkrs': tuple(tickers),
    }

    # Execute query
    tickers_df = pd.read_sql(query, con, params=query_params)


    return tickers_df
def populate_prices(out1_df, prices_df ):
    out_df = out1_df
    res_series = out_df.apply(get_prices_data, axis=1, result_type='expand', args=(prices_df,))

    out_df = pd.concat([out_df, res_series], axis=1)

    return out_df


def get_prices_data(row, prices_df):

    ticker = row['Ticker']
    print(f"populating prices for ticker:{ticker}")
    date1 = row['d_one']
    d_one_price = get_price(prices_df, date1, ticker)
    date1 = row['d_two']
    d_two_price = get_price(prices_df, date1, ticker)
    date1 = row['d_three']
    d_three_price = get_price(prices_df, date1, ticker)
    date1 = row['d_week1']
    d_week1_price = get_price(prices_df, date1, ticker)
    date1 = row['d_week2']
    d_week2_price = get_price(prices_df, date1, ticker)
    date1 = row['d_week3']
    d_week3_price = get_price(prices_df, date1, ticker)
    date1 = row['d_month1']
    d_month1_price = get_price(prices_df, date1, ticker)
    date1 = row['d_month2']
    d_month2_price = get_price(prices_df, date1, ticker)
    date1 = row['d_quarter']
    d_quarter_price = get_price(prices_df, date1, ticker)

    date1 = row['de_one']
    de_one_price = get_price(prices_df, date1, ticker)
    date1 = row['de_two']
    de_two_price = get_price(prices_df, date1, ticker)
    date1 = row['de_three']
    de_three_price = get_price(prices_df, date1, ticker)
    date1 = row['de_week1']
    de_week1_price = get_price(prices_df, date1, ticker)
    date1 = row['de_week2']
    de_week2_price = get_price(prices_df, date1, ticker)
    date1 = row['de_week3']
    de_week3_price = get_price(prices_df, date1, ticker)
    date1 = row['de_month1']
    de_month1_price = get_price(prices_df, date1, ticker)
    date1 = row['de_month2']
    de_month2_price = get_price(prices_df, date1, ticker)
    date1 = row['de_quarter']
    de_quarter_price = get_price(prices_df, date1, ticker)


    prices_list = [d_one_price, d_two_price, d_three_price, d_week1_price, d_week2_price, d_week3_price, d_month1_price, d_month2_price, d_quarter_price, de_one_price, de_two_price, de_three_price, de_week1_price, de_week2_price, de_week3_price, de_month1_price, de_month2_price, de_quarter_price]
    index_names = ["d_one_price", "d_two_price", "d_three_price", "d_week1_price", "d_week2_price", "d_week3_price", "d_month1_price", "d_month2_price", "d_quarter_price", "de_one_price", "de_two_price", "de_three_price", "de_week1_price", "de_week2_price", "de_week3_price", "de_month1_price", "de_month2_price", "de_quarter_price"]

    prices_series = pd.Series(prices_list,index=index_names)

    return prices_series

def get_price(prices_df,datee,tkr):

    filtered = prices_df.query("ticker == @tkr and date == @datee")
    price = filtered.iloc[0]['close']

    return price

def populate_maxmin_prices(out1_df,tickers_df):
    out_df = out1_df
    res_series = out_df.apply(get_ticker_prices_data, axis=1, result_type='expand', args=(tickers_df,))

    out_df = pd.concat([out_df, res_series], axis=1)

    return out_df

def get_ticker_prices_data(row, tickers_df):

    ticker = row['Ticker']
    date1 = row['Entry Date']
    date2 = row['FinalExitDate']
    date3 = row['exit_date2']

    tickers_df = tickers_df[tickers_df['ticker'] == ticker]

    print(f"populating all prices for ticker:{ticker}")

    max1_price = get_max_price(tickers_df, date1, date2)
    max2_price = get_max_price(tickers_df, date1, date3)

    min1_price = get_min_price(tickers_df, date1, date2)
    min2_price = get_min_price(tickers_df, date1, date3)


    maxmin_list = [max1_price, max2_price, min1_price, min2_price]
    index_names = ["max1_price", "max2_price", "min1_price", "min2_price"]

    prices_series = pd.Series(maxmin_list,index=index_names)

    return prices_series

def get_max_price(tickers_df,date_beg,date_end):
    date_beg = pd.Timestamp(date_beg)
    date_end = pd.Timestamp(date_end)

    max_price = tickers_df.loc[tickers_df['date'].between(date_beg, date_end), 'high'].max()

    return max_price

def get_min_price(tickers_df,date_beg,date_end):
    date_beg = pd.Timestamp(date_beg)
    date_end = pd.Timestamp(date_end)

    min_price = tickers_df.loc[tickers_df['date'].between(date_beg, date_end), 'low'].min()

    return min_price

def calculate_metrics(out_df):

    res_series = out_df.apply(get_ticker_metrics_data, axis=1, result_type='expand')

    out_df = pd.concat([out_df, res_series], axis=1)

    return out_df

def get_ticker_metrics_data(row):

    atr = row['ATR']
    tradeR = row['T-P&L( R )']

    if row['Direction'] == 'Long':
        unitR = row['Entry Price'] - row['OPS Level']
        tradeR_A = (row['FinalExitPrice'] - row['Entry Price']) / atr
    else:
        unitR = row['OPS Level'] - row['Entry Price']
        tradeR_A = -(row['FinalExitPrice'] - row['Entry Price']) / atr




    if row['Direction'] == 'Long':
        R1 = (row['d_one_price'] - row['Entry Price'])/unitR
        RX1 = (row['de_one_price'] - row['Entry Price'])/unitR
        RA1 = (row['d_one_price'] - row['Entry Price'])/atr
        RAX1 = (row['de_one_price'] - row['Entry Price'])/atr

        R2 = (row['d_two_price'] - row['Entry Price']) / unitR
        RX2 = (row['de_two_price'] - row['Entry Price']) / unitR
        RA2 = (row['d_two_price'] - row['Entry Price']) / atr
        RAX2 = (row['de_two_price'] - row['Entry Price']) / atr

        R3 = (row['d_three_price'] - row['Entry Price']) / unitR
        RX3 = (row['de_three_price'] - row['Entry Price']) / unitR
        RA3 = (row['d_three_price'] - row['Entry Price']) / atr
        RAX3 = (row['de_three_price'] - row['Entry Price']) / atr

        RW1 = (row['d_week1_price'] - row['Entry Price']) / unitR
        RXW1 = (row['de_week1_price'] - row['Entry Price']) / unitR
        RAW1 = (row['d_week1_price'] - row['Entry Price']) / atr
        RAXW1 = (row['de_week1_price'] - row['Entry Price']) / atr

        RW2 = (row['d_week2_price'] - row['Entry Price']) / unitR
        RXW2 = (row['de_week2_price'] - row['Entry Price']) / unitR
        RAW2 = (row['d_week2_price'] - row['Entry Price']) / atr
        RAXW2 = (row['de_week2_price'] - row['Entry Price']) / atr

        RW3 = (row['d_week3_price'] - row['Entry Price']) / unitR
        RXW3 = (row['de_week3_price'] - row['Entry Price']) / unitR
        RAW3 = (row['d_week3_price'] - row['Entry Price']) / atr
        RAXW3 = (row['de_week3_price'] - row['Entry Price']) / atr

        RM1 = (row['d_month1_price'] - row['Entry Price']) / unitR
        RXM1 = (row['de_month1_price'] - row['Entry Price']) / unitR
        RAM1 = (row['d_month1_price'] - row['Entry Price']) / atr
        RAXM1 = (row['de_month1_price'] - row['Entry Price']) / atr

        RM2 = (row['d_month2_price'] - row['Entry Price']) / unitR
        RXM2 = (row['de_month2_price'] - row['Entry Price']) / unitR
        RAM2 = (row['d_month2_price'] - row['Entry Price']) / atr
        RAXM2 = (row['de_month2_price'] - row['Entry Price']) / atr

        RQ1 = (row['d_quarter_price'] - row['Entry Price']) / unitR
        RXQ1 = (row['de_quarter_price'] - row['Entry Price']) / unitR
        RAQ1 = (row['d_quarter_price'] - row['Entry Price']) / atr
        RAXQ1 = (row['de_quarter_price'] - row['Entry Price']) / atr

        MFE1 = (row['max1_price'] - row['Entry Price']) / unitR
        MAE1 = (row['min1_price'] - row['Entry Price']) / unitR
        MFE2 = (row['max2_price'] - row['Entry Price']) / unitR
        MAE2 = (row['min2_price'] - row['Entry Price']) / unitR

        MFE1_A = (row['max1_price'] - row['Entry Price']) / atr
        MAE1_A = (row['min1_price'] - row['Entry Price']) / atr
        MFE2_A = (row['max2_price'] - row['Entry Price']) / atr
        MAE2_A = (row['min2_price'] - row['Entry Price']) / atr

    else:

        R1 = -(row['d_one_price'] - row['Entry Price']) / unitR
        RX1 = -(row['de_one_price'] - row['Entry Price']) / unitR
        RA1 = -(row['d_one_price'] - row['Entry Price']) / atr
        RAX1 = -(row['de_one_price'] - row['Entry Price']) / atr

        R2 = -(row['d_two_price'] - row['Entry Price']) / unitR
        RX2 = -(row['de_two_price'] - row['Entry Price']) / unitR
        RA2 = -(row['d_two_price'] - row['Entry Price']) / atr
        RAX2 = -(row['de_two_price'] - row['Entry Price']) / atr

        R3 = -(row['d_three_price'] - row['Entry Price']) / unitR
        RX3 = -(row['de_three_price'] - row['Entry Price']) / unitR
        RA3 = -(row['d_three_price'] - row['Entry Price']) / atr
        RAX3 = -(row['de_three_price'] - row['Entry Price']) / atr

        RW1 = -(row['d_week1_price'] - row['Entry Price']) / unitR
        RXW1 = -(row['de_week1_price'] - row['Entry Price']) / unitR
        RAW1 = -(row['d_week1_price'] - row['Entry Price']) / atr
        RAXW1 = -(row['de_week1_price'] - row['Entry Price']) / atr

        RW2 = -(row['d_week2_price'] - row['Entry Price']) / unitR
        RXW2 = -(row['de_week2_price'] - row['Entry Price']) / unitR
        RAW2 = -(row['d_week2_price'] - row['Entry Price']) / atr
        RAXW2 = -(row['de_week2_price'] - row['Entry Price']) / atr

        RW3 = -(row['d_week3_price'] - row['Entry Price']) / unitR
        RXW3 = -(row['de_week3_price'] - row['Entry Price']) / unitR
        RAW3 = -(row['d_week3_price'] - row['Entry Price']) / atr
        RAXW3 = -(row['de_week3_price'] - row['Entry Price']) / atr

        RM1 = -(row['d_month1_price'] - row['Entry Price']) / unitR
        RXM1 = -(row['de_month1_price'] - row['Entry Price']) / unitR
        RAM1 = -(row['d_month1_price'] - row['Entry Price']) / atr
        RAXM1 = -(row['de_month1_price'] - row['Entry Price']) / atr

        RM2 = -(row['d_month2_price'] - row['Entry Price']) / unitR
        RXM2 = -(row['de_month2_price'] - row['Entry Price']) / unitR
        RAM2 = -(row['d_month2_price'] - row['Entry Price']) / atr
        RAXM2 = -(row['de_month2_price'] - row['Entry Price']) / atr

        RQ1 = -(row['d_quarter_price'] - row['Entry Price']) / unitR
        RXQ1 = -(row['de_quarter_price'] - row['Entry Price']) / unitR
        RAQ1 = -(row['d_quarter_price'] - row['Entry Price']) / atr
        RAXQ1 = -(row['de_quarter_price'] - row['Entry Price']) / atr

        MFE1 = -(row['max1_price'] - row['Entry Price']) / unitR
        MAE1 = -(row['min1_price'] - row['Entry Price']) / unitR
        MFE2 = -(row['max2_price'] - row['Entry Price']) / unitR
        MAE2 = -(row['min2_price'] - row['Entry Price']) / unitR

        MFE1_A = -(row['max1_price'] - row['Entry Price']) / atr
        MAE1_A = -(row['min1_price'] - row['Entry Price']) / atr
        MFE2_A = -(row['max2_price'] - row['Entry Price']) / atr
        MAE2_A = -(row['min2_price'] - row['Entry Price']) / atr

    metrics_list = [tradeR, R1, R2, R3, RW1, RW2, RW3, RM1, RM2, RQ1, RX1, RX2, RX3, RXW1, RXW2, RXW3, RXM1, RXM2, RXQ1, MFE1, MFE2, MAE1, MAE2, tradeR_A, RA1, RA2, RA3, RAW1, RAW2, RAW3, RAM1, RAM2, RAQ1, RAX1, RAX2, RAX3, RAXW1, RAXW2, RAXW3, RAXM1, RAXM2, RAXQ1, MFE1_A, MFE2_A, MAE1_A, MAE2_A]
    index_names = ['tradeR', 'R1', 'R2', 'R3', 'RW1', 'RW2', 'RW3', 'RM1', 'RM2', 'RQ1', 'RX1', 'RX2', 'RX3', 'RXW1', 'RXW2', 'RXW3', 'RXM1', 'RXM2', 'RXQ1', 'MFE1', 'MFE2', 'MAE1', 'MAE2', 'tradeR_A', 'RA1', 'RA2', 'RA3', 'RAW1', 'RAW2', 'RAW3', 'RAM1', 'RAM2', 'RAQ1', 'RAX1', 'RAX2', 'RAX3', 'RAXW1', 'RAXW2', 'RAXW3', 'RAXM1', 'RAXM2', 'RAXQ1','MFE1_A', 'MFE2_A', 'MAE1_A', 'MAE2_A']

    metrics_series = pd.Series(metrics_list, index=index_names)

    return metrics_series





def update_excel(excel_file,df):
    # Output file has Prices after entry and exit date: 1 ,2, 3; 5; 10; 15; 22; 44; 66 (so 18 prices in total) - we will calculate the change w.r.t risk (R) and daily ATR (A)
    # Also, we will calculate MAE, MFE for the holding period and twice the holding period - necessary for the calculating the capture rate

    in_df =  pd.read_excel(excel_file)
    df_subset = df[['S.No', 'tradeR', 'R1', 'R2', 'R3', 'RW1', 'RW2', 'RW3', 'RM1', 'RM2', 'RQ1', 'RX1', 'RX2', 'RX3', 'RXW1', 'RXW2', 'RXW3', 'RXM1', 'RXM2', 'RXQ1', 'MFE1', 'MFE2', 'MAE1', 'MAE2', 'tradeR_A', 'RA1', 'RA2', 'RA3', 'RAW1', 'RAW2', 'RAW3', 'RAM1', 'RAM2', 'RAQ1', 'RAX1', 'RAX2', 'RAX3', 'RAXW1', 'RAXW2', 'RAXW3', 'RAXM1', 'RAXM2', 'RAXQ1','MFE1_A', 'MFE2_A', 'MAE1_A', 'MAE2_A']]

    merged = in_df.merge(df_subset, on = 'S.No', how='left')

    merged = merged.round(2)

    merged.to_excel(excel_file)





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
    print("getting valid dates")
    valid_dates = get_valid_dates(con)
    print("done getting valid dates")


    in_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Newsletters\DailyNarratives\Tradelog-baseline.xlsx"


    in_df = read_input_data(in_file)
    out_df = populate_required_dates(in_df, valid_dates)
    prices_df = get_prices_df(con,out_df)
    tickers_df = get_tickers_df(con,out_df)
    out_df = populate_prices(out_df,prices_df)
    out_df = populate_maxmin_prices(out_df,tickers_df)
    out_df = calculate_metrics(out_df)


    update_excel(in_file, out_df)

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")
