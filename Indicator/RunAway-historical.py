# Read an excel file with a list of tickers and dates, go to the key indicators table, get the runaway counts prior to that date

import pandas as pd
import psycopg2
import sys
import time
from datetime import datetime, timedelta


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


def get_prior_date(df, specific_date):


    # Find the index of the specific_date in the DataFrame
    date_index = df[df['date'] == specific_date.strftime('%Y-%m-%d')].index[0]

    # Check if there's a prior date
    if date_index > 0:
        prior_date = df.iloc[date_index - 1]['date']
        return prior_date
    else:
        return None  # No prior date available

def read_tickers(excel_file):
    # read excel file in to pandas data frame- which has ticker, direction, entry_date
    excel_df = pd.read_excel(excel_file)

    return excel_df

def get_runaway_counts(conn, in_df, vd_df):
    # get columns from db table

    out_df = in_df
    res_df = in_df.apply(get_runaway_data, axis=1, result_type='expand', args=(conn,vd_df,))

    out_df = pd.concat([out_df, res_df], axis=1)

    return out_df

    
def get_runaway_data(row,conn, vd_df):
    tkr = row['ticker']
    date1 = row['entry_date'].date()
    datee = get_prior_date(vd_df,date1)
    cursor = conn.cursor()

    try:
        print(f"Processing for ticker {tkr} for date {datee}")

        postgreSQL_select_Query = """
           SELECT ticker, date, runaway_up_521, runaway_up_1030, runaway_up_0205,
           runaway_down_521, runaway_down_1030, runaway_down_0205
           FROM key_indicators_alltickers_sinceapril2022_view
           WHERE ticker = %(tkr)s AND date = %(dobject)s
           """

        query_params = {'tkr': tkr, 'dobject': datee}
        cursor.execute(postgreSQL_select_Query, query_params)

        record = cursor.fetchone()

        if record is not None:
            df = pd.Series(record, index=[desc[0] for desc in cursor.description])

            # Select and calculate the desired columns
            df = df[['ticker', 'date', 'runaway_up_521', 'runaway_up_1030', 'runaway_up_0205',
                     'runaway_down_521', 'runaway_down_1030', 'runaway_down_0205']]

            df['Net0521'] = df['runaway_up_521'] - df['runaway_down_521']
            df['Net1030'] = df['runaway_up_1030'] - df['runaway_down_1030']
            df['Net0205'] = df['runaway_up_0205'] - df['runaway_down_0205']

            return df
        else:
            print(f"No data found for ticker {tkr} on date {datee}")
            return None

    except Exception as e:
        print(f"Error processing ticker {tkr} on date {datee}: {str(e)}")
        return None

    finally:
        cursor.close()
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
    print("getting valid dates")
    valid_dates = get_valid_dates(con)
    print("done getting valid dates")


    in_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily Task\daily work\Daily_Tech_Reading\Runaway_historical_infile.xlsx"
    out_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily Task\daily work\Daily_Tech_Reading\Runaway_historical_opfile.xlsx"


    in_df = read_tickers(in_file)

    out_df = get_runaway_counts(con,in_df, valid_dates)


    update_excel(out_df, out_file)

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")
