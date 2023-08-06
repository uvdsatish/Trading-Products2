# stock reading performance and trades not taken analysis
#  max and min return, trade duration(cal days); Great/Good/Loser; Quickest, Quick, Slow {Win/Loss};  # GreatQuickest #GreatQuick #Great Slow GooqQuickest Good quick Good Slow LoserSlow; perf update: max of max return;
#  Min of Min return;  #trades not taken(greatQuickest+greatQuick+greatSlow+goodQuickest+goodQuick); #LoserSlow, separate data frame for trades not taken; check rounding to decimal digits
#  Active-inactive (crossing 200MA) and corresponding dates, ATR stop based trading
#  project 2: reversal bars - cgc etc; Historical; Delta (adding rows - new days/data, adding new indicators)
# this can be used/modified/updated for trade log to performance assessment
import pandas as pd
import psycopg2
from datetime import datetime
import sys
import itertools
import logging
import concurrent.futures

import numpy as np


base_dir = "D:/Trading Dropbox/Satish Udayagiri/SatishUdayagiri/Trading/Process/StockReading-2023"

# Create a custom logger
logger = logging.getLogger(__name__)

# Set level of logger
logger.setLevel(logging.INFO)

# Create file handler which logs even debug messages
fh = logging.FileHandler(f'{base_dir}/shorts_analysis-{str(datetime.now().date())}.log')
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
    init_df = init_df[['date', 'Ticker', 'Direction',
                       'Source', 'status', 'status_date', 'activeDuration']]
    # Adding subsequent dates assuming date identified day is a friday
    init_df['NextDayDate'] = init_df['date'] + pd.Timedelta(days=3)
    init_df['WeekAheadDate'] = init_df['date'] + pd.Timedelta(days=7)
    init_df['MonthAheadDate'] = init_df['date'] + pd.Timedelta(days=30)
    init_df['quarterAheadDate'] = init_df['date'] + pd.Timedelta(days=91)

    init_df.date = init_df.date.apply(lambda x: x.date())
    init_df.NextDayDate = init_df.NextDayDate.apply(lambda x: x.date())
    init_df.WeekAheadDate = init_df.WeekAheadDate.apply(lambda x: x.date())
    init_df.MonthAheadDate = init_df.MonthAheadDate.apply(lambda x: x.date())
    init_df.quarterAheadDate = init_df.quarterAheadDate.apply(
        lambda x: x.date())

    return init_df


def get_valid_dates(con):
    # get valid trading dates
    cursor = con.cursor()
    select_query = "select timestamp from valid_trading_dates"
    cursor.execute(select_query)
    valid_dates = cursor.fetchall()

    df = pd.DataFrame(valid_dates, columns=['date'])

    return df


def nearest(valid_dates_list, date_filtered):
    # find the nearest trading day
    return min(valid_dates_list, key=lambda x: abs(x - date_filtered))


def update_dates(init_df, valid_dates_list):
    # modify holidays to nearest trading day
    init_df.date = init_df.date.apply(lambda x: nearest(valid_dates_list, x))
    init_df.NextDayDate = init_df.NextDayDate.apply(
        lambda x: nearest(valid_dates_list, x))
    init_df.WeekAheadDate = init_df.WeekAheadDate.apply(
        lambda x: nearest(valid_dates_list, x))
    init_df.MonthAheadDate = init_df.MonthAheadDate.apply(
        lambda x: nearest(valid_dates_list, x))
    init_df.quarterAheadDate = init_df.quarterAheadDate.apply(
        lambda x: nearest(valid_dates_list, x))

    return init_df


def get_allprice_data(con):
    # get all price data
    cursor = con.cursor()
    select_query = "select * from usstockseod_sincedec2021_view"
    cursor.execute(select_query)
    all_price = cursor.fetchall()

    df = pd.DataFrame(all_price,
                      columns=['Ticker', 'timestamp', 'high', 'low', 'open', 'close', 'volume', 'openinterest'])

    return df


def update_prices(init_df, allprice_df):
    # Prepare for merging by melting 'init_df' 
    melt_df = init_df.melt(id_vars=['Ticker'], 
                           value_vars=['date', 'NextDayDate', 'WeekAheadDate', 'MonthAheadDate', 'quarterAheadDate'], 
                           var_name='DateType', value_name='Date')

    # Merge with allprice_df
    merged_df = melt_df.merge(allprice_df[['Date', 'Ticker', 'close', 'open']], on=['Date', 'Ticker'], how='left')

    # Pivot merged_df to have date types as columns
    pivot_df_close = merged_df.pivot_table(index=['Ticker', 'Date'], columns='DateType', values='close').reset_index()
    pivot_df_open = merged_df.pivot_table(index=['Ticker', 'Date'], columns='DateType', values='open').reset_index()

    # Rename columns as per your code
    new_names_close = {
        'date': 'fridayClose', 
        'NextDayDate': 'mondayClose', 
        'WeekAheadDate': 'weekAheadClose',
        'MonthAheadDate': 'monthAheadClose',
        'quarterAheadDate': 'quarterAheadClose'
    }
    pivot_df_close.rename(columns=new_names_close, inplace=True)

    new_names_open = {
        'NextDayDate': 'mondayOpen',
    }
    pivot_df_open.rename(columns=new_names_open, inplace=True)

    # Join pivot_df back with init_df
    result_df = init_df.merge(pivot_df_close, on='Ticker', how='left')
    result_df = result_df.merge(pivot_df_open[['Ticker', 'Date', 'mondayOpen']], on=['Ticker', 'Date'], how='left')

    result_df = result_df[~result_df['mondayClose'].isnull()]
    result_df.reset_index(drop=True, inplace=True)

    return result_df


def update_returns(init_df, dir):
    if dir == "Long":
        init_df['dailyReturn'] = round(
            (init_df['mondayClose'] - init_df['mondayOpen']) * 100 / init_df['mondayOpen'], 2)
        init_df['dailyReturn2'] = round(
            (init_df['mondayClose'] - init_df['fridayClose']) * 100 / init_df['fridayClose'], 2)
        init_df['weeklyReturn'] = round(
            (init_df['weekAheadClose'] - init_df['mondayOpen']) * 100 / init_df['mondayOpen'], 2)
        init_df['monthlyReturn'] = round(
            (init_df['monthAheadClose'] - init_df['mondayOpen']) * 100 / init_df['mondayOpen'], 2)
        init_df['quarterlyReturn'] = round((init_df['quarterAheadClose'] - init_df['mondayOpen']) * 100 / init_df[
            'mondayOpen'], 2)
    elif dir == "Short":
        init_df['dailyReturn'] = round(
            (init_df['mondayOpen'] - init_df['mondayClose']) * 100 / init_df['mondayOpen'], 2)
        init_df['dailyReturn2'] = round(
            (init_df['fridayClose'] - init_df['mondayClose']) * 100 / init_df['fridayClose'], 2)
        init_df['weeklyReturn'] = round(
            (init_df['mondayOpen'] - init_df['weekAheadClose']) * 100 / init_df['mondayOpen'], 2)
        init_df['monthlyReturn'] = round(
            (init_df['mondayOpen'] - init_df['monthAheadClose']) * 100 / init_df['mondayOpen'], 2)
        init_df['quarterlyReturn'] = round((init_df['mondayOpen'] - init_df['quarterAheadClose']) * 100 / init_df[
            'mondayOpen'], 2)
    else:
        logger.error("wrong direction")
        sys.exit(1)

    return init_df


def update_output_excel(init_df, direction, source, output_files_dict):
    if source == "Mark" and direction == "Long":
        init_df.to_excel(output_files_dict["mark_long_file_op"])
    elif source == "Mark" and direction == "Short":
        init_df.to_excel(output_files_dict["mark_short_file_op"])
    elif source == "Satish" and direction == "Short":
        init_df.to_excel(output_files_dict["satish_short_file_op"])
    elif source == "Satish" and direction == "Long":
        init_df.to_excel(output_files_dict["satish_long_file_op"])
    elif source == "SPY" and direction == "Long":
        init_df.to_excel(output_files_dict["spy_long_file_op"])
    elif source == "SPY" and direction == "Short":
        init_df.to_excel(output_files_dict["spy_short_file_op"])
    elif source == "QQQ" and direction == "Long":
        init_df.to_excel(output_files_dict["qqq_long_file_op"])
    elif source == "QQQ" and direction == "Short":
        init_df.to_excel(output_files_dict["qqq_short_file_op"])
    elif source == "IWM" and direction == "Long":
        init_df.to_excel(output_files_dict["iwm_long_file_op"])
    elif source == "IWM" and direction == "Short":
        init_df.to_excel(output_files_dict["iwm_short_file_op"])
    elif source == "MDY" and direction == "Long":
        init_df.to_excel(output_files_dict["mdy_long_file_op"])
    elif source == "MDY" and direction == "Short":
        init_df.to_excel(output_files_dict["mdy_short_file_op"])
    elif source == "FFTY" and direction == "Long":
        init_df.to_excel(output_files_dict["ffty_long_file_op"])
    elif source == "FFTY" and direction == "Short":
        init_df.to_excel(output_files_dict["ffty_short_file_op"])
    else:
        logger.error("wrong source or direction")
        sys.exit(1)


def highestHigh(date1, ticker, status_dt, allp_df):
    if date1.size == ticker.size:
        temp_df = pd.DataFrame(
            columns=['NextDayDate', 'Ticker', 'status_date'])
        temp_df['NextDayDate'] = date1
        temp_df['Ticker'] = ticker
        temp_df['status_date'] = status_dt
    else:
        logger.error("dates and tickers size mismatching")
        sys.exit(1)
    counter = itertools.count(0)
    high_list = [getHigh(x, y, z, allp_df, next(counter)) for x, y, z in zip(
        temp_df['NextDayDate'], temp_df['Ticker'], temp_df['status_date'])]
    return high_list


def getHigh(nextdaydate, ticker, status_date, allp_df, count):
    logging.info("trade %s" % count)
    logging.info("ticker %s" % ticker)
    # we need to change the code here, if we are making some trades inactive
    allpm_df = allp_df.loc[(allp_df.index.get_level_values(0) == ticker) & (
        allp_df.index.get_level_values(1) >= nextdaydate) & (allp_df.index.get_level_values(1) <= status_date)]
    if len(allpm_df) == 0:
        logging.info("ticker %s data is not found between dates %s and %s" %
              (ticker, nextdaydate, status_date))
        h_list = [0, 0]
    else:
        hh = allpm_df['high'].max()
        hd = allpm_df.at[allpm_df['high'].idxmax(), 'Date']
        hh = round(hh, 2)
        h_list = [hh, hd]
    return h_list


def lowestLow(date1, ticker, status_dt, allp_df):
    if date1.size == ticker.size:
        temp_df = pd.DataFrame(
            columns=['NextDayDate', 'Ticker', 'status_date'])
        temp_df['NextDayDate'] = date1
        temp_df['Ticker'] = ticker
        temp_df['status_date'] = status_dt
    else:
        logging.info("dates and tickers size mismatching")
        sys.exit(1)
    counter = itertools.count(0)
    low_list = [getLow(x, y, z, allp_df, next(counter)) for x, y, z in zip(
        temp_df['NextDayDate'], temp_df['Ticker'], temp_df['status_date'])]
    return low_list


def getLow(nextdaydate, ticker, status_date, allp_df, count):
    logging.info("trade %s" % count)
    allpm_df = allp_df.loc[(allp_df.index.get_level_values(0) == ticker) & (
        allp_df.index.get_level_values(1) >= nextdaydate) & (allp_df.index.get_level_values(1) <= status_date)]
    if len(allpm_df) == 0:
        logging.info("ticker %s data is not found between dates %s and %s" %
              (ticker, nextdaydate, status_date))
        l_list = [0, 0]
    else:
        ll = allpm_df['low'].min()
        ld = allpm_df.at[allpm_df['low'].idxmin(), 'Date']
        ll = round(ll, 2)
        l_list = [ll, ld]
    return l_list


def split_columns(orig_df, col):
    split_df = pd.DataFrame(orig_df[col].tolist(), columns=[
                            col+"Mod", col+"Date"])
    orig_df = pd.concat([orig_df, split_df], axis=1)
    orig_df = orig_df.drop(col, axis=1)
    orig_df = orig_df.rename(columns={col+"Mod": col})
    return orig_df


def update_returns_duration(init_df, dir):
    if dir == "Long":
        init_df['maxReturn'] = round(
            (init_df['HighestHigh'] - init_df['mondayOpen']) * 100 / init_df['mondayOpen'], 2)
        init_df['minReturn'] = round(
            (init_df['LowestLow'] - init_df['mondayOpen']) * 100 / init_df['mondayOpen'], 2)
        init_df['tradeDurationMaxReturn'] = (
            (init_df['HighestHighDate'] - init_df['NextDayDate']) / np.timedelta64(1, 'D'))
        init_df['tradeDurationMinReturn'] = (
            (init_df['LowestLowDate'] - init_df['NextDayDate']) / np.timedelta64(1, 'D'))
    elif dir == "Short":
        init_df['maxReturn'] = round(
            (init_df['mondayOpen'] - init_df['LowestLow']) * 100 / init_df['mondayOpen'], 2)
        init_df['minReturn'] = round(
            (init_df['mondayOpen'] - init_df['HighestHigh']) * 100 / init_df['mondayOpen'], 2)
        init_df['tradeDurationMaxReturn'] = (
            (init_df['LowestLowDate'] - init_df['NextDayDate']) / np.timedelta64(1, 'D'))
        init_df['tradeDurationMinReturn'] = (
            (init_df['HighestHighDate'] - init_df['NextDayDate']) / np.timedelta64(1, 'D'))
    else:
        print("wrong direction")
        sys.exit(1)

    return init_df


def update_trades_quality(init_df):
    great = 30
    good = 15
    loss = 1
    init_df['tradeQuality'] = ""

    init_df['tradeQuality'] = np.where(
        init_df.maxReturn >= great, "Great", init_df.tradeQuality)
    init_df['tradeQuality'] = np.where(((init_df.maxReturn >= good) & (init_df.maxReturn < great)), "Good",
                                       init_df.tradeQuality)
    init_df['tradeQuality'] = np.where(((init_df.maxReturn >= loss) & (init_df.maxReturn < good)), "Ok",
                                       init_df.tradeQuality)
    init_df['tradeQuality'] = np.where(
        init_df.maxReturn < loss, "Losing", init_df.tradeQuality)

    quickest = 30
    quick = 60
    slow = 90

    init_df['tradeSpeed'] = ""

    init_df['tradeSpeed'] = np.where(
        init_df.tradeDurationMaxReturn <= quickest, "SuperFast", init_df.tradeSpeed)
    init_df['tradeSpeed'] = np.where(
        ((init_df.tradeDurationMaxReturn <= quick) &
         (init_df.tradeDurationMaxReturn > quickest)), "Fast",
        init_df.tradeSpeed)
    init_df['tradeSpeed'] = np.where(
        ((init_df.tradeDurationMaxReturn <= slow) &
         (init_df.tradeDurationMaxReturn > quick)), "Average",
        init_df.tradeSpeed)
    init_df['tradeSpeed'] = np.where(
        init_df.tradeDurationMaxReturn > slow, "Slow", init_df.tradeSpeed)

    return init_df


def update_performance(init_df, source, direction):
    perf_dict = {}

    perf_dict["source"] = source
    perf_dict["direction"] = direction

    currDate = datetime.now().date()

    init_df_daily = init_df.loc[init_df['date']
                                < (currDate - pd.Timedelta(days=3))]
    init_df_weekly = init_df.loc[init_df['date']
                                 < (currDate - pd.Timedelta(days=7))]
    init_df_monthly = init_df.loc[init_df['date']
                                  < (currDate - pd.Timedelta(days=30))]
    init_df_quarterly = init_df.loc[init_df['date'] < (
        currDate - pd.Timedelta(days=91))]

    perf_dict["dailyReturnCount"] = (init_df_daily.shape)[0]
    perf_dict["weeklyReturnCount"] = (init_df_weekly.shape)[0]
    perf_dict["monthlyReturnCount"] = (init_df_monthly.shape)[0]
    perf_dict["quarterlyReturnCount"] = (init_df_quarterly.shape)[0]

    perf_dict["dailyReturnMean"] = round(
        init_df_daily['dailyReturn'].mean(), 2)
    perf_dict["dailyReturn2Mean"] = round(
        init_df_daily['dailyReturn2'].mean(), 2)
    perf_dict["weeklyReturnMean"] = round(
        init_df_weekly['weeklyReturn'].mean(), 2)
    perf_dict["monthlyReturnMean"] = round(
        init_df_monthly['monthlyReturn'].mean(), 2)
    perf_dict["quarterlyReturnMean"] = round(
        init_df_quarterly['quarterlyReturn'].mean(), 2)
    perf_dict["maxReturnMean"] = round(
        init_df_quarterly['maxReturn'].mean(), 2)
    perf_dict["minReturnMean"] = round(
        init_df_quarterly['minReturn'].mean(), 2)

    perf_dict["dailyReturnMedian"] = round(
        init_df_daily['dailyReturn'].median(), 2)
    perf_dict["dailyReturn2Median"] = round(
        init_df_daily['dailyReturn2'].median(), 2)
    perf_dict["weeklyReturnMedian"] = round(
        init_df_weekly['weeklyReturn'].median(), 2)
    perf_dict["monthlyReturnMedian"] = round(
        init_df_monthly['monthlyReturn'].median(), 2)
    perf_dict["quarterlyReturnMedian"] = round(
        init_df_quarterly['quarterlyReturn'].median(), 2)
    perf_dict["maxReturnMedian"] = round(
        init_df_quarterly['maxReturn'].median(), 2)
    perf_dict["minReturnMedian"] = round(
        init_df_quarterly['minReturn'].median(), 2)

    perf_dict["dailyReturnMax"] = round(init_df_daily['dailyReturn'].max(), 2)
    perf_dict["dailyReturn2Max"] = round(
        init_df_daily['dailyReturn2'].max(), 2)
    perf_dict["weeklyReturnMax"] = round(
        init_df_weekly['weeklyReturn'].max(), 2)
    perf_dict["monthlyReturnMax"] = round(
        init_df_monthly['monthlyReturn'].max(), 2)
    perf_dict["quarterlyReturnMax"] = round(
        init_df_quarterly['quarterlyReturn'].max(), 2)
    perf_dict["maxReturnMax"] = round(init_df_quarterly['maxReturn'].max(), 2)
    perf_dict["minReturnMax"] = round(init_df_quarterly['minReturn'].max(), 2)

    perf_dict["dailyReturnMin"] = round(init_df_daily['dailyReturn'].min(), 2)
    perf_dict["dailyReturn2Min"] = round(
        init_df_daily['dailyReturn2'].min(), 2)
    perf_dict["weeklyReturnMin"] = round(
        init_df_weekly['weeklyReturn'].min(), 2)
    perf_dict["monthlyReturnMin"] = round(
        init_df_monthly['monthlyReturn'].min(), 2)
    perf_dict["quarterlyReturnMin"] = round(
        init_df_quarterly['quarterlyReturn'].min(), 2)
    perf_dict["maxReturnMin"] = round(init_df_quarterly['maxReturn'].min(), 2)
    perf_dict["minReturnMin"] = round(init_df_quarterly['minReturn'].min(), 2)

    perf_dict["greatTradesCount"] = (
        init_df.loc[init_df["tradeQuality"] == "Great"]).shape[0]
    perf_dict["goodTradesCount"] = (
        init_df.loc[init_df["tradeQuality"] == "Good"]).shape[0]
    perf_dict["okTradesCount"] = (
        init_df.loc[init_df["tradeQuality"] == "Ok"]).shape[0]
    perf_dict["losingTradesCount"] = (
        init_df.loc[init_df["tradeQuality"] == "Losing"]).shape[0]

    perf_dict["superfastTradesCount"] = (
        init_df.loc[init_df["tradeSpeed"] == "SuperFast"]).shape[0]
    perf_dict["fastTradesCount"] = (
        init_df.loc[init_df["tradeSpeed"] == "Fast"]).shape[0]
    perf_dict["averageTradesCount"] = (
        init_df.loc[init_df["tradeSpeed"] == "Average"]).shape[0]
    perf_dict["slowTradesCount"] = (
        init_df.loc[init_df["tradeSpeed"] == "Slow"]).shape[0]

    perf_dict["greatsuperfastTradesCount"] = \
        (init_df.loc[(init_df["tradeQuality"] == "Great") &
         (init_df["tradeSpeed"] == "SuperFast")]).shape[0]
    perf_dict["greatfastTradesCount"] = \
        (init_df.loc[(init_df["tradeQuality"] == "Great")
         & (init_df["tradeSpeed"] == "Fast")]).shape[0]
    perf_dict["greataverageTradesCount"] = \
        (init_df.loc[(init_df["tradeQuality"] == "Great")
         & (init_df["tradeSpeed"] == "Average")]).shape[0]
    perf_dict["greatslowTradesCount"] = \
        (init_df.loc[(init_df["tradeQuality"] == "Great")
         & (init_df["tradeSpeed"] == "Slow")]).shape[0]
    perf_dict["goodsuperfastTradesCount"] = \
        (init_df.loc[(init_df["tradeQuality"] == "Good") &
         (init_df["tradeSpeed"] == "SuperFast")]).shape[0]
    perf_dict["goodfastTradesCount"] = \
        (init_df.loc[(init_df["tradeQuality"] == "Good")
         & (init_df["tradeSpeed"] == "Fast")]).shape[0]

    perf_dict["totalAwesomeTrades"] = perf_dict["greatsuperfastTradesCount"] + perf_dict["greatfastTradesCount"] + \
        perf_dict["greataverageTradesCount"] + perf_dict["greatslowTradesCount"] + \
        perf_dict["goodsuperfastTradesCount"] + \
        perf_dict["goodfastTradesCount"]

    perf_dict["dailyWinRate"] = round(
        init_df_daily.loc[init_df_daily['dailyReturn'] >= 0, 'dailyReturn'].count() * 100 / (
            init_df_daily['dailyReturn'].count()), 2)
    perf_dict["dailyWinRate2"] = round(
        init_df_daily.loc[init_df_daily['dailyReturn2'] >= 0, 'dailyReturn2'].count() * 100 / (
            init_df_daily['dailyReturn'].count()), 2)
    perf_dict["weeklyWinRate"] = round(
        init_df_weekly.loc[init_df_weekly['weeklyReturn'] >= 0, 'weeklyReturn'].count() * 100 / (
            init_df_weekly['weeklyReturn'].count()), 2)
    perf_dict["monthlyWinRate"] = round(
        init_df_monthly.loc[init_df_monthly['monthlyReturn'] >= 0, 'monthlyReturn'].count() * 100 / (
            init_df_monthly['monthlyReturn'].count()), 2)
    perf_dict["quarterlyWinRate"] = round(
        init_df_quarterly.loc[init_df_quarterly['quarterlyReturn'] >= 0, 'quarterlyReturn'].count() * 100 / (
            init_df['quarterlyReturn'].count()), 2)

    return perf_dict


def process_file(file, valid_dates_list, allprice_df, output_files_dict):
    init_df = read_data(file)
    # Take only those trades where active duration is greater than 0

    init_df = init_df[init_df['activeDuration'] > 0]
    init_df.reset_index(drop=True, inplace=True)

    if len(init_df) == 0:
        return

    direction = init_df.at[0, "Direction"]
    source = init_df.at[0, "Source"]
    logger.info("processing file %s %s" % (source, direction))

    logger.info("updating dates")
    init_df = update_dates(init_df, valid_dates_list)

    logger.info("updating prices")
    init_df = update_prices(init_df, allprice_df)

    allprice_df['Date'] = allprice_df.index.get_level_values(1)
    direction = init_df.at[0, "Direction"]
    source = init_df.at[0, "Source"]

    logger.info("updating returns")
    init_df = update_returns(init_df, direction)

    logger.info("updating HighestHigh")
    init_df = init_df.assign(HighestHigh=lambda x: highestHigh(
        x['NextDayDate'], x['Ticker'], x['status_date'], allprice_df))
    init_df = split_columns(init_df, "HighestHigh")

    init_df['HighestHighDate'] = pd.to_datetime(init_df['HighestHighDate'])
    init_df['NextDayDate'] = pd.to_datetime(init_df['NextDayDate'])

    logger.info("updating LowestLow")
    init_df = init_df.assign(LowestLow=lambda x: lowestLow(
        x['NextDayDate'], x['Ticker'], x['status_date'], allprice_df))
    init_df = split_columns(init_df, "LowestLow")

    init_df['LowestLowDate'] = pd.to_datetime(init_df['LowestLowDate'])

    logger.info("update max and min returns and trade duration")
    init_df = update_returns_duration(init_df, direction)

    logger.info("update trade quality and speed")
    init_df = update_trades_quality(init_df)

    init_df['Awesome Trade'] = (init_df["tradeQuality"] == "Great") | ((init_df["tradeQuality"] == "Good") & (
        (init_df["tradeSpeed"] == "SuperFast") | (init_df["tradeSpeed"] == "Fast")))

    update_output_excel(init_df, direction, source, output_files_dict)

    perf_dict = update_performance(init_df, source, direction)
    return perf_dict


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

    logger.info("Connected - now getting valid dates")
    valid_dates = get_valid_dates(con)

    valid_dates.date = valid_dates["date"].apply(lambda x: x.date())
    valid_dates = valid_dates.sort_values(by="date", ascending=True)
    valid_dates_list = valid_dates['date'].tolist()

    logger.info("got valid dates -- now getting all price data")
    allprice_df = get_allprice_data(con)
    allprice_df['basedate'] = allprice_df.timestamp.apply(lambda x: x.date())

    allprice_df.set_index(['Ticker', 'basedate'], drop=True, inplace=True)
    allprice_df.index.sortlevel(level=0, sort_remaining=True)

    satish_long_file = f"{base_dir}/StockReadingMetrics-Satish-Long-int.xlsx"
    satish_short_file = f"{base_dir}/StockReadingMetrics-Satish-Short-int.xlsx"
    mark_long_file = f"{base_dir}/StockReadingMetrics-Mark-Long-int.xlsx"
    mark_short_file = f"{base_dir}/StockReadingMetrics-Mark-Short-int.xlsx"

    spy_long_file = f"{base_dir}/StockReadingMetrics-SPY-Long-int.xlsx"
    spy_short_file = f"{base_dir}/StockReadingMetrics-SPY-Short-int.xlsx"
    qqq_long_file = f"{base_dir}/StockReadingMetrics-QQQ-Long-int.xlsx"
    qqq_short_file = f"{base_dir}/StockReadingMetrics-QQQ-Short-int.xlsx"
    iwm_long_file = f"{base_dir}/StockReadingMetrics-IWM-Long-int.xlsx"
    iwm_short_file = f"{base_dir}/StockReadingMetrics-IWM-Short-int.xlsx"
    mdy_long_file = f"{base_dir}/StockReadingMetrics-MDY-Long-int.xlsx"
    mdy_short_file = f"{base_dir}/StockReadingMetrics-MDY-Short-int.xlsx"
    ffty_long_file = f"{base_dir}/StockReadingMetrics-FFTY-Long-int.xlsx"
    ffty_short_file = f"{base_dir}/StockReadingMetrics-FFTY-Short-int.xlsx"

    output_files_dict = {
        "satish_long_file_op": f"{base_dir}/StockReadingMetrics-Satish-Long-op.xlsx",
        "satish_short_file_op": f"{base_dir}/StockReadingMetrics-Satish-Short-op.xlsx",
        "mark_long_file_op": f"{base_dir}/StockReadingMetrics-Mark-Long-op.xlsx",
        "mark_short_file_op": f"{base_dir}/StockReadingMetrics-Mark-Short-op.xlsx",
        "spy_long_file_op": f"{base_dir}/StockReadingMetrics-SPY-Long-op.xlsx",
        "spy_short_file_op": f"{base_dir}/StockReadingMetrics-SPY-Short-op.xlsx",
        "qqq_long_file_op": f"{base_dir}/StockReadingMetrics-QQQ-Long-op.xlsx",
        "qqq_short_file_op": f"{base_dir}/StockReadingMetrics-QQQ-Short-op.xlsx",
        "iwm_long_file_op": f"{base_dir}/StockReadingMetrics-IWM-Long-op.xlsx",
        "iwm_short_file_op": f"{base_dir}/StockReadingMetrics-IWM-Short-op.xlsx",
        "mdy_long_file_op": f"{base_dir}/StockReadingMetrics-MDY-Long-op.xlsx",
        "mdy_short_file_op": f"{base_dir}/StockReadingMetrics-MDY-Short-op.xlsx",
        "ffty_long_file_op": f"{base_dir}/StockReadingMetrics-FFTY-Long-op.xlsx",
        "ffty_short_file_op": f"{base_dir}/StockReadingMetrics-FFTY-Short-op.xlsx"
    }

    final_perf_summary = f"{base_dir}/StockReadingMetrics-perf-summary.xlsx"

    input_files_list = [satish_long_file, satish_short_file, mark_long_file, mark_short_file, spy_long_file, spy_short_file,
                        qqq_long_file, qqq_short_file, iwm_long_file, iwm_short_file, mdy_long_file, mdy_short_file, ffty_long_file, ffty_short_file]
    # input_files_list = [spy_short_file]
    perf_dict_list = []

    logger.info('Processing in parallel')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        perf_dict_list = list(executor.map(process_file, input_files_list,
                                           [valid_dates_list] *
                                           len(input_files_list),
                                           [allprice_df] * len(input_files_list),
                                           [output_files_dict] * len(input_files_list)))

    # Filter out any None values that were returned
    perf_dict_list = [
        perf_dict for perf_dict in perf_dict_list if perf_dict is not None]

    perf_df = pd.DataFrame.from_dict(perf_dict_list)
    perf_df.to_excel(final_perf_summary)

    con.close()
