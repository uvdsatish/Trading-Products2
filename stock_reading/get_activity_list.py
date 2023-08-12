import datetime
import sys
import glob
from datetime import datetime
import os
import pandas as pd
from util import pg_connect


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
    # get all price data
    cursor = con.cursor()
    select_query = "select * from usstockseod_sincedec2020_view"
    cursor.execute(select_query)
    all_price_cursor = cursor.fetchall()

    allprice_df = pd.DataFrame(all_price_cursor,
                               columns=['Ticker', 'timestamp', 'high', 'low', 'open', 'close', 'volume', 'openinterest', 'dma50', 'vma30'])

    allprice_df['date'] = allprice_df.timestamp.apply(lambda x: x.date())
    allprice_df.set_index(['Ticker', 'date'], drop=True, inplace=True)
    allprice_df.index.sortlevel(level=0, sort_remaining=True)

    return allprice_df


def update_input_file(init_df, valid_dates_list, allprice_df):

    init_df = update_dates(init_df, valid_dates_list)
    init_df.rename(columns={'DateIdentified': 'date'}, inplace=True)
    init_df = init_df.merge(allprice_df, on=['date', 'Ticker'], how='left')
    init_df.drop(columns=['timestamp', 'high', 'low', 'open',
                 'volume', 'openinterest', 'dma50', 'vma30'], inplace=True)
    init_df.rename(columns={"close": "entryPrice"}, inplace=True)
    init_df["status"] = "Active"
    init_df["status_date"] = datetime.today().date()

    return init_df


def prepare_ticker_data(allp_df):
    # what does this do?
    return {
        ticker: df
        for ticker, df in allp_df.groupby(level=0)
    }


def update_status_date(ticker, date, direction, allprice_df):

    ticker_data = prepare_ticker_data(allprice_df)

    results = pd.DataFrame(
        {'entry_date': date, 'ticker': ticker, 'direction': direction})
    results['status_and_date'] = results.apply(
        lambda row: get_status_and_date(row['entry_date'], row['ticker'], row['direction'], ticker_data), axis=1)

    return results['status_and_date'].tolist()


def get_status_and_date(entryDate, ticker, direction, ticker_data):

    if ticker not in ticker_data:
        print(f"Ticker{ticker} data is not found")
        return [0, 0]

    df = ticker_data[ticker]

    mask = (pd.to_datetime(df.index.get_level_values(1))
            >= pd.to_datetime(entryDate))

    subset = df[mask]

    if subset.empty:
        print(f"Ticker {ticker} data is not found after the date {entryDate} ")
        return [0, 0]

    subset["status"] = 0
    subset["date"] = subset.timestamp.apply(lambda x: x.date())

    if direction == "Long":
        subset["status"] = ((subset["close"] < subset["dma50"]) & (
            subset["volume"] > subset["vma30"]))
    elif direction == "Short":
        subset["status"] = ((subset["close"] > subset["dma50"]) & (
            subset["volume"] > subset["vma30"]))
    else:
        print("incorrect direction")
        sys.exit(1)

    if subset['status'].any():
        status = "Inactive"
    else:
        status = "Active"

    if status == "Inactive":
        status_date = subset.loc[subset['status'].idxmax(), 'date']
    else:
        status_date = datetime.today().date()

    return [status, status_date]


def update_int_excel(init_df, direction, source, int_files_dict):
    try:
        init_df.to_excel(
            int_files_dict[f"{source}_{direction}_file_int".lower()])
    except:
        print("wrong source or direction")
        sys.exit(1)


if __name__ == '__main__':
    con = pg_connect()
    data_base_dir = os.environ.get('DATA_BASE_DIR')
    input_files_list = glob.glob(
        f"{data_base_dir}/StockReading-2023/*-ip.xlsx")

    def get_int_file_key(file):
        file_ = file.split('/')[-1].split('.')[0].lower().split('-')
        return f'{file_[1]}_{file_[2]}_file_int'

    int_files_dict = dict(map(lambda file: (get_int_file_key(
        file), file.replace('-ip.xlsx', '-int.xlsx')), input_files_list))

    print("Connected - now getting valid dates")
    valid_dates_list = get_valid_dates(con)

    print("got valid dates -- now getting all price data")
    allprice_df = get_allprice_data(con)

    # Use below for loop as reference and convert to multi processing logic
    for file in input_files_list:
        print(f'Processing {file}')
        init_df = read_data(file)
        init_df = update_input_file(init_df, valid_dates_list, allprice_df)
        init_df = update_status_date(init_df.copy(), allprice_df.copy())
        init_df["activeDuration"] = init_df["status_date"] - init_df["date"]
        init_df_active = init_df[init_df["status"] == 'Active']

        direction = init_df.at[0, "Direction"]
        source = init_df.at[0, "Source"]

        update_int_excel(init_df, direction, source, int_files_dict)
