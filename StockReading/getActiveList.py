import pandas as pd
import psycopg2
import datetime
import sys
from datetime import datetime
import numpy as np

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

def read_data(file):
    init_df = pd.read_excel(file)
    init_df = init_df[['date', 'Ticker', 'Direction', 'Source']]

    init_df.date = init_df.date.apply(lambda x: x.date())

    return init_df

def get_valid_dates(con):
    # get valid trading dates
    cursor = con.cursor()
    select_query = "select timestamp from valid_trading_dates"
    cursor.execute(select_query)
    valid_dates_cursor = cursor.fetchall()

    valid_dates = pd.DataFrame(valid_dates_cursor, columns=['date'])

    valid_dates.date = valid_dates["date"].apply(lambda x: x.date())
    valid_dates = valid_dates.sort_values(by="date", ascending=True)
    valid_dates_list = valid_dates['date'].tolist()

    return valid_dates_list

def nearest(valid_dates_list, date_filtered):
    # find the nearest trading day
    return min(valid_dates_list, key=lambda x: abs(x - date_filtered))


def update_dates(init_df, valid_dates_list):
    # modify holidays to nearest trading day
    init_df.date = init_df.date.apply(lambda x: nearest(valid_dates_list, x))

    return init_df

def get_allprice_data(con):
    #get all price data
    cursor = con.cursor()
    select_query = "select * from usstockseod_sincedec2020_view"
    cursor.execute(select_query)
    all_price_cursor = cursor.fetchall()

    allprice_df = pd.DataFrame(all_price_cursor,
                      columns=['Ticker', 'timestamp', 'high', 'low', 'open', 'close', 'volume', 'openinterest','dma50','vma30'])

    allprice_df['date'] = allprice_df.timestamp.apply(lambda x: x.date())
    allprice_df = allprice_df.drop_duplicates(subset=['Ticker','date'], keep='last')
    allprice_df.set_index(['Ticker', 'date'], drop=True, inplace=True)
    allprice_df.index.sortlevel(level=0, sort_remaining=True)

    return allprice_df

def update_input_file(init_df, valid_dates_list, allprice_df):

    init_df = update_dates(init_df, valid_dates_list)
    init_df.rename(columns={'DateIdentified': 'date'}, inplace=True)
    init_df = init_df.merge(allprice_df, on=['date', 'Ticker'], how='left')
    init_df.drop(columns=['timestamp', 'high', 'low', 'open', 'volume', 'openinterest', 'dma50', 'vma30'], inplace=True)
    init_df.rename(columns={"close": "entryPrice"}, inplace=True)
    init_df["status"] = "Active"
    init_df["status_date"] = datetime.today().date()

    return init_df


def update_status_date(ticker, date, direction, allprice_df):

    ticker_data = prepare_ticker_data(allprice_df)

    results = pd.DataFrame({'entryDate': date, 'Ticker': ticker, 'Direction': direction})
    results['statusanddate'] = results.apply(
        lambda row: get_statusanddate(row['entryDate'], row['Ticker'], row['Direction'], ticker_data), axis=1)

    return results['statusanddate'].tolist()



def get_statusanddate(entryDate,ticker,direction,ticker_data):

    if ticker not in ticker_data:
        print(f"Ticker{ticker} data is not found")
        return [0, 0]

    df = ticker_data[ticker]

    mask = (pd.to_datetime(df.index.get_level_values(1)) >= pd.to_datetime(entryDate))

    subset = df[mask]

    if subset.empty:
        print(f"Ticker {ticker} data is not found after the date {entryDate} ")
        return [0, 0]

    subset["status"] = 0
    subset["date"] = subset.timestamp.apply(lambda x: x.date())

    if direction == "Long":
        subset["status"] = ((subset["close"] < subset["dma50"]) & (subset["volume"] > subset["vma30"]))
    elif direction == "Short":
        subset["status"] = ((subset["close"] > subset["dma50"]) & (subset["volume"] > subset["vma30"]))
    else:
        print("incorrect direction")
        sys.exit(1)


    if subset['status'].any():
        status = "InActive"
    else:
        status = "Active"

    if status == "InActive":
        status_date = subset.loc[subset['status'].idxmax(),'date']
    else:
        status_date = datetime.today().date()

    return [status, status_date]


def prepare_ticker_data(allp_df):
    # what does this do?
    return {
        ticker: df
        for ticker, df in allp_df.groupby(level = 0)
    }


def split_columns(orig_df,col):
    orig_df = orig_df.drop(columns = ["status", "status_date"], axis=1)
    split_df = pd.DataFrame(orig_df[col].tolist(),columns=["status", "status_date"])
    orig_df = pd.concat([orig_df,split_df], axis=1)
    orig_df = orig_df.drop(columns=["statusanddate"], axis=1)

    return orig_df



def update_int_excel(init_df,direction, source, int_files_dict):
    if source=="Mark" and direction == "Long":
        init_df.to_excel(int_files_dict["mark_long_file_int"])
    elif source=="Mark" and direction == "Short":
        init_df.to_excel(int_files_dict["mark_short_file_int"])
    elif source=="Satish" and direction == "Short":
        init_df.to_excel(int_files_dict["satish_short_file_int"])
    elif source=="Satish" and direction == "Long":
        init_df.to_excel(int_files_dict["satish_long_file_int"])
    elif source=="SPY" and direction == "Long":
        init_df.to_excel(int_files_dict["spy_long_file_int"])
    elif source=="SPY" and direction == "Short":
        init_df.to_excel(int_files_dict["spy_short_file_int"])
    elif source=="QQQ" and direction == "Long":
        init_df.to_excel(int_files_dict["qqq_long_file_int"])
    elif source=="QQQ" and direction == "Short":
        init_df.to_excel(int_files_dict["qqq_short_file_int"])
    elif source=="IWM" and direction == "Long":
        init_df.to_excel(int_files_dict["iwm_long_file_int"])
    elif source=="IWM" and direction == "Short":
        init_df.to_excel(int_files_dict["iwm_short_file_int"])
    elif source=="MDY" and direction == "Long":
        init_df.to_excel(int_files_dict["mdy_long_file_int"])
    elif source=="MDY" and direction == "Short":
        init_df.to_excel(int_files_dict["mdy_short_file_int"])
    elif source=="FFTY" and direction == "Long":
        init_df.to_excel(int_files_dict["ffty_long_file_int"])
    elif source=="FFTY" and direction == "Short":
        init_df.to_excel(int_files_dict["ffty_short_file_int"])
    else:
        print("wrong source or direction")
        sys.exit(1)


if __name__ == '__main__':

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    satish_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-Satish-Long-ip.xlsx"
    satish_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-Satish-Short-ip.xlsx"
    mark_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-Mark-Long-ip.xlsx"
    mark_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-Mark-Short-ip.xlsx"
    spy_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-SPY-Long-ip.xlsx"
    spy_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-SPY-Short-ip.xlsx"
    qqq_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-QQQ-Long-ip.xlsx"
    qqq_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-QQQ-Short-ip.xlsx"
    iwm_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-IWM-Long-ip.xlsx"
    iwm_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-IWM-Short-ip.xlsx"
    mdy_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-MDY-Long-ip.xlsx"
    mdy_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-MDY-Short-ip.xlsx"
    ffty_long_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-FFTY-Long-ip.xlsx"
    ffty_short_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\InputFiles\StockReadingMetrics-FFTY-Short-ip.xlsx"

    input_files_list = [satish_long_file, satish_short_file, mark_long_file, mark_short_file, spy_long_file,
                        spy_short_file, qqq_long_file, qqq_short_file, iwm_long_file, iwm_short_file, mdy_long_file,
                        mdy_short_file, ffty_long_file, ffty_short_file]

    #input_files_list = [ spy_short_file, qqq_short_file,  iwm_short_file,
    #                    mdy_short_file, ffty_short_file]

    int_files_dict = {
        "satish_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-Satish-Long-int.xlsx",
        "satish_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-Satish-Short-int.xlsx",
        "mark_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-Mark-Long-int.xlsx",
        "mark_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-Mark-Short-int.xlsx",
        "spy_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-SPY-Long-int.xlsx",
        "spy_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-SPY-Short-int.xlsx",
        "qqq_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-QQQ-Long-int.xlsx",
        "qqq_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-QQQ-Short-int.xlsx",
        "iwm_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-IWM-Long-int.xlsx",
        "iwm_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-IWM-Short-int.xlsx",
        "mdy_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-MDY-Long-int.xlsx",
        "mdy_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-MDY-Short-int.xlsx",
        "ffty_long_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-FFTY-Long-int.xlsx",
        "ffty_short_file_int": r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\StockReading\StockReading-2024-April\StockReadingMetrics-FFTY-Short-int.xlsx"
    }

    print(" getting valid dates")

    valid_dates_list = get_valid_dates(con)
    print(" getting price data")

    allprice_df = get_allprice_data(con)

    for file in input_files_list:
        print(f"Processing file : {file}")
        init_df = read_data(file)
        init_df = update_input_file(init_df, valid_dates_list, allprice_df)

        init_df = init_df.assign(statusanddate=lambda x: update_status_date(x['Ticker'], x['date'], x['Direction'], allprice_df))

        init_df = split_columns(init_df, "statusanddate")
        init_df['date'] = init_df['date'].apply(pd.to_datetime)
        init_df['status_date'] = init_df['status_date'].apply(pd.to_datetime)

        init_df["activeDuration"] = (init_df["status_date"] - init_df["date"]).dt.days

        direction = init_df.at[0, "Direction"]
        source = init_df.at[0, "Source"]

        update_int_excel(init_df, direction, source, int_files_dict)




    

    


    

    
