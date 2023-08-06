import pandas as pd
import psycopg2
import datetime
import sys
import logging
from datetime import datetime
import concurrent.futures


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        logger.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        sys.exit(1)
    logger.info("Connection successful")
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


def update_status_date(init_df, allprice_df):
    for index, row in init_df.iterrows():
        logger.info('getting status for ticker %s for date %s' %
              (row["Ticker"], row["date"]))
        status_date_list = get_status_date(
            row["Ticker"], row["date"], row["Direction"], allprice_df)
        init_df.at[index, 'status'] = status_date_list[0]
        init_df.at[index, 'status_date'] = status_date_list[1]

    return init_df


def get_status_date(ticker, entryDate, direction, allprice_df):
    tmp_df = allprice_df.loc[
        (allprice_df.index.get_level_values(0) == ticker) & (allprice_df.index.get_level_values(1) >= entryDate) & (
            allprice_df.index.get_level_values(1) <= datetime.today().date())]
    tmp_df.timestamp = tmp_df.timestamp.apply(lambda x: x.date())
    status = "Active"
    status_date = datetime.today().date()
    if direction == "Long":
        for row in tmp_df.iterrows():
            if ((row['close'] < row['dma50']) & (row['volume'] > row['vma30'])):
                status = "inactive"
                status_date = row['timestamp']
                break
    elif direction == "Short":
        for row in tmp_df.iterrows():
            if ((row['close'] > row['dma50']) & (row['volume'] > row['vma30'])):
                status = "inactive"
                status_date = row['timestamp']
                break
    else:
        logger.error("Incorrect direction")

    return [status, status_date]


def update_int_excel(init_df, direction, source, int_files_dict):
    if source == "Mark" and direction == "Long":
        init_df.to_excel(int_files_dict["mark_long_file_int"])
    elif source == "Mark" and direction == "Short":
        init_df.to_excel(int_files_dict["mark_short_file_int"])
    elif source == "Satish" and direction == "Short":
        init_df.to_excel(int_files_dict["satish_short_file_int"])
    elif source == "Satish" and direction == "Long":
        init_df.to_excel(int_files_dict["satish_long_file_int"])
    elif source == "SPY" and direction == "Long":
        init_df.to_excel(int_files_dict["spy_long_file_int"])
    elif source == "SPY" and direction == "Short":
        init_df.to_excel(int_files_dict["spy_short_file_int"])
    elif source == "QQQ" and direction == "Long":
        init_df.to_excel(int_files_dict["qqq_long_file_int"])
    elif source == "QQQ" and direction == "Short":
        init_df.to_excel(int_files_dict["qqq_short_file_int"])
    elif source == "IWM" and direction == "Long":
        init_df.to_excel(int_files_dict["iwm_long_file_int"])
    elif source == "IWM" and direction == "Short":
        init_df.to_excel(int_files_dict["iwm_short_file_int"])
    elif source == "MDY" and direction == "Long":
        init_df.to_excel(int_files_dict["mdy_long_file_int"])
    elif source == "MDY" and direction == "Short":
        init_df.to_excel(int_files_dict["mdy_short_file_int"])
    elif source == "FFTY" and direction == "Long":
        init_df.to_excel(int_files_dict["ffty_long_file_int"])
    elif source == "FFTY" and direction == "Short":
        init_df.to_excel(int_files_dict["ffty_short_file_int"])
    else:
        logger.info("wrong source or direction")
        sys.exit(1)


def process_file(file, valid_dates_list, allprice_df, int_files_dict):
    init_df = read_data(file)
    init_df = update_input_file(init_df, valid_dates_list, allprice_df)
    init_df = update_status_date(init_df, allprice_df)
    init_df["activeDuration"] = init_df["status_date"] - init_df["date"]

    direction = init_df.at[0, "Direction"]
    source = init_df.at[0, "Source"]

    update_int_excel(init_df, direction, source, int_files_dict)


if __name__ == '__main__':
    base_dir = "D:/Trading Dropbox/Satish Udayagiri/SatishUdayagiri/Trading/Process/StockReading-2023"

    # Create a custom logger
    logger = logging.getLogger(__name__)

    # Set level of logger
    logger.setLevel(logging.INFO)

    # Create file handler which logs even debug messages
    fh = logging.FileHandler(f'{base_dir}/getActiveList-{str(datetime.datetime.now().date())}.log')
    fh.setLevel(logging.INFO)

    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    satish_long_file = f"{base_dir}/StockReadingMetrics-Satish-Long-ip.xlsx"
    satish_short_file = f"{base_dir}/StockReadingMetrics-Satish-Short-ip.xlsx"
    mark_long_file = f"{base_dir}/StockReadingMetrics-Mark-Long-ip.xlsx"
    mark_short_file = f"{base_dir}/StockReadingMetrics-Mark-Short-ip.xlsx"

    spy_long_file = f"{base_dir}/StockReadingMetrics-SPY-Long-ip.xlsx"
    spy_short_file = f"{base_dir}/StockReadingMetrics-SPY-Short-ip.xlsx"
    qqq_long_file = f"{base_dir}/StockReadingMetrics-QQQ-Long-ip.xlsx"
    qqq_short_file = f"{base_dir}/StockReadingMetrics-QQQ-Short-ip.xlsx"
    iwm_long_file = f"{base_dir}/StockReadingMetrics-IWM-Long-ip.xlsx"
    iwm_short_file = f"{base_dir}/StockReadingMetrics-IWM-Short-ip.xlsx"
    mdy_long_file = f"{base_dir}/StockReadingMetrics-MDY-Long-ip.xlsx"
    mdy_short_file = f"{base_dir}/StockReadingMetrics-MDY-Short-ip.xlsx"
    ffty_long_file = f"{base_dir}/StockReadingMetrics-FFTY-Long-ip.xlsx"
    ffty_short_file = f"{base_dir}/StockReadingMetrics-FFTY-Short-ip.xlsx"

    input_files_list = [spy_short_file, qqq_short_file,  iwm_short_file,
                        mdy_short_file, ffty_short_file]

    int_files_dict = {
        "satish_long_file_int": f"{base_dir}/StockReadingMetrics-Satish-Long-int.xlsx",
        "satish_short_file_int": f"{base_dir}/StockReadingMetrics-Satish-Short-int.xlsx",
        "mark_long_file_int": f"{base_dir}/StockReadingMetrics-Mark-Long-int.xlsx",
        "mark_short_file_int": f"{base_dir}/StockReadingMetrics-Mark-Short-int.xlsx",
        "spy_long_file_int": f"{base_dir}/StockReadingMetrics-SPY-Long-int.xlsx",
        "spy_short_file_int": f"{base_dir}/StockReadingMetrics-SPY-Short-int.xlsx",
        "qqq_long_file_int": f"{base_dir}/StockReadingMetrics-QQQ-Long-int.xlsx",
        "qqq_short_file_int": f"{base_dir}/StockReadingMetrics-QQQ-Short-int.xlsx",
        "iwm_long_file_int": f"{base_dir}/StockReadingMetrics-IWM-Long-int.xlsx",
        "iwm_short_file_int": f"{base_dir}/StockReadingMetrics-IWM-Short-int.xlsx",
        "mdy_long_file_int": f"{base_dir}/StockReadingMetrics-MDY-Long-int.xlsx",
        "mdy_short_file_int": f"{base_dir}/StockReadingMetrics-MDY-Short-int.xlsx",
        "ffty_long_file_int": f"{base_dir}/StockReadingMetrics-FFTY-Long-int.xlsx",
        "ffty_short_file_int": f"{base_dir}/StockReadingMetrics-FFTY-Short-int.xlsx"
    }

    valid_dates_list = get_valid_dates(con)

    allprice_df = get_allprice_data(con)

    logger.info('Processing in parallel')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(
            process_file, input_files_list,
            [valid_dates_list] *
            len(input_files_list),
            [allprice_df] * len(input_files_list),
            [int_files_dict] * len(input_files_list)
        )
