import pandas as pd
import psycopg2
import sys
import talib as ta
from io import StringIO
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

    return df


def get_allprice_data(con):
    cursor = con.cursor()
    select_query = "select * from usstockseod"
    cursor.execute(select_query)
    valid_dates = cursor.fetchall()

    df = pd.DataFrame(valid_dates,
                      columns=['Ticker', 'timestamp', 'high', 'low', 'open', 'close', 'volume', 'openinterest'])

    return df


def calculate_moving_average(df):
    df["SMA10"] = ta.SMA(df["close"], timeperiod=10)
    df["SMA50"] = ta.SMA(df["close"], timeperiod=50)
    df["SMA200"] = ta.SMA(df["close"], timeperiod=200)
    df["EMA20"] = ta.EMA(df["close"], timeperiod=20)

    return df

def calculate_ATR(df):
    df["ATR10"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=10)
    df["ATR14"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=14)
    df["ATR20"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=20)

    return df

def BB_band(df):

    df["BB_upper"] = ta.BBANDS(df["close"], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[0]
    df["BB_lower"] = ta.BBANDS(df["close"], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[1]

    return df

def KC_band(df):

    df["KCmiddle"] = df["EMA20"]
    df["KCupper"]  = df["KCmiddle"] + 2*df["ATR10"]
    df["KClower"] = df["KCmiddle"] - 2*df["ATR10"]

    return df

def volume_MA(df):

    df["VMA30"] = ta.MA(df["volume"], timeperiod=30)
    df["VMA50"] = ta.MA(df["volume"], timeperiod=50)
    df["VMA63"] = ta.MA(df["volume"], timeperiod=63)

    return df

def yearly_high(df):

    df["52WHigh"] = ta.MAX(df["high"], timeperiod=252)

    return df

def yearly_low(df):

    df["52WLow"] = ta.MIN(df["low"], timeperiod=252)

    return df

def adx(df):

    df["ADX14"] = ta.ADX(df["high"], df["low"], df["close"], timeperiod=14)
    df["ADX20"] = ta.ADX(df["high"], df["low"], df["close"], timeperiod=20)

    return df

def macd(df):

    df["MACD"], df["MACDsignal"], df["MACDhist"] = ta.MACD(df["close"], fastperiod=12, slowperiod=26, signalperiod=9)

    return df

def rsi(df):

    df["RSI14"] = ta.RSI(df["close"], timeperiod=14)
    df["RSI20"] = ta.RSI(df["close"], timeperiod=20)

    return df

def stoch(df):

    df["STOCH14"] = ta.STOCH(df["high"], df["low"], df["close"], fastk_period=14, slowk_period=3, slowd_period=3)[0]
    df["STOCH20"] = ta.STOCH(df["high"], df["low"], df["close"], fastk_period=20, slowk_period=3, slowd_period=3)[0]

    return df

def OBV(df):

    df["OBV"] = ta.OBV(df["close"], df["volume"])

    return df

def reversals(df):

    df['reversal_bull'] = (df['low'] < df['low'].shift(periods=1)) & (
                df['close'] > df['open']) & ((df['volume'] > df['volume'].shift(periods=1)) | (
                df['volume'] > df['VMA30']))
    df['reversal_bear'] = (df['high'] > df['high'].shift(periods=1)) & (
                df['close'] < df['open']) & ((df['volume'] > df['volume'].shift(periods=1)) | (
                df['volume'] > df['VMA30']))

    return df

def key_reversals(df):

    df['key_reversal_bull'] = (df['low'] < df['low'].shift(periods=1)) & (
                df['close'] > df['close'].shift(periods=1)) & (df['close'] > df['open']) & (
                                               (df['volume'] > df['volume'].shift(periods=1)) | (
                                                   df['volume'] > df['VMA30']))
    df['key_reversal_bear'] = (df['high'] > df['high'].shift(periods=1)) & (
                df['close'] < df['close'].shift(periods=1)) & (df['close'] < df['open']) & (
                                               (df['volume'] > df['volume'].shift(periods=1)) | (
                                                   df['volume'] > df['VMA30']))

    return df

def trueRange(df):

    df["TR"] = ta.TRANGE(df["high"], df["low"], df["close"])

    return df

def cpr(df):

    df['cpr_bull'] = (df['close'] > df['high'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['VMA30']))
    df['cpr_bear'] = (df['close'] < df['low'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['VMA30']))

    return df

def cgc(df):

    df['cgc_bull'] = (df['close'] > df['high'].shift(periods=1)) & (
                df['open'] > df['close'].shift(periods=1)) & (
                                      df['low'] > df['close'].shift(periods=1)) & (
                                      (df['volume'] > df['volume'].shift(periods=1)) | (
                                          df['volume'] > df['VMA30']))
    df['cgc_bear'] = (df['close'] < df['low'].shift(periods=1)) & (
                df['open'] < df['close'].shift(periods=1)) & (
                                      df['high'] < df['close'].shift(periods=1)) & (
                                      (df['volume'] > df['volume'].shift(periods=1)) | (
                                          df['volume'] > df['VMA30']))
    return df

def outside_bar(df):

    df['outside_bar_up'] = (df['close'] > df['open']) & (
                df['low'] < df['low'].shift(periods=1)) & (
                                            df['high'] > df['high'].shift(periods=1)) & (
                                            (df['high'] - df['close']) < 0.33 * (
                                                df['high'] - df['low'])) & (
                                            (df['volume'] > df['volume'].shift(periods=1)) | (
                                                df['volume'] > df['VMA30']))
    df['outside_bar_down'] = (df['close'] < df['open']) & (
                df['low'] < df['low'].shift(periods=1)) & (
                                              df['high'] > df['high'].shift(periods=1)) & (
                                              (df['close'] - df['low']) < 0.33 * (
                                                  df['high'] - df['low'])) & (
                                              (df['volume'] > df['volume'].shift(periods=1)) | (
                                                  df['volume'] > df['VMA30']))
    return df

def inside_bar(df):

    df['inside_bar'] = (df['low'] > df['low'].shift(periods=1)) & (
                df['high'] < df['high'].shift(periods=1))

    return df

def signal_bar(df):

    df['signal_bar_bull'] = (df['open'] < df['low'].shift(periods=1)) & (
                df['close'] > df['open']) & ((df['high'] - df['close']) < 0.2 * (
                df['high'] - df['low']))
    df['signal_bar_bear'] = (df['open'] > df['high'].shift(periods=1)) & (
                df['close'] < df['open']) & ((df['close'] - df['low']) < 0.2 * (
                df['high'] - df['low']))
    
    return df

def noRange(df):

    df['NR7'] = (df['TR'] < df['TR'].shift(periods=1)) & (df['TR'] < df['TR'].shift(periods=2)) & (
                df['TR'] < df['TR'].shift(periods=3)) & (df['TR'] < df['TR'].shift(periods=4)) & (
                            df['TR'] < df['TR'].shift(periods=5)) & (df['TR'] < df['TR'].shift(periods=6)) & (
                            df['TR'] < df['TR'].shift(periods=7))

    return df

def tr3(df):

    df['TR3'] = (df['TR'] < 0.5 * df['ATR14']) & (
                df['TR'].shift(periods=1) < 0.5 * df['ATR14'].shift(periods=1)) & (
                                 df['TR'].shift(periods=2) < 0.5 * df['ATR14'].shift(periods=2))

    return df

def stalling_bear(df):

    df['stalling_bear'] = (df['volume'] > df['volume'].shift(periods=1)) & (
                df['close'] > df['close'].shift(periods=1)) & ((df['high'] - df['close']) > 0.5 * (
                df['high'] - df['low'])) & ((df['high'] - df['open']) < 0.2 * (
                df['high'] - df['low']))

    return df

def LRHCHV(df):

    df['LRHCHV'] = (df['TR'] > 1.75 * df['ATR14']) & (
                (df['high'] - df['close']) < 0.2 * (df['high'] - df['low'])) & (
                                    df['close'] > df['open']) & (
                                    (df['volume'] > df['volume'].shift(periods=1)) & (
                                        df['volume'] > df['VMA30']))
    df['LRLCHV'] = (df['TR'] > 1.75 * df['ATR14']) & (
                (df['close'] - df['low']) < 0.2 * (df['high'] - df['low'])) & (
                                    df['close'] < df['open']) & (
                                    (df['volume'] > df['volume'].shift(periods=1)) & (
                                        df['volume'] > df['VMA30']))

    return df

def LVLR(df):

    df['LV_day'] = df['volume'] < 0.5 * df['VMA30']
    df['LR_day'] = df['TR'] < 0.6 * df['ATR14']

    return df

def exhaust(df):

    df['exhaust_bar_up'] = (df['TR'] > 2 * df['ATR14']) & (
                df['volume'] > 1.5 * df['ATR14']) & (df['close'] > df['open'])
    df['exhaust_bar_down'] = (df['TR'] > 2 * df['ATR14']) & (
                df['volume'] > 1.5 * df['ATR14']) & (df['close'] < df['open'])
    df['exhaust_condition_bull'] = (df['exhaust_bar_up'].shift(periods=1) == True) & (
                df['close'] > df['close'].shift(periods=1))
    df['exhaust_condition_bear'] = (df['exhaust_bar_down'].shift(periods=1) == True) & (
                df['close'] < df['close'].shift(periods=1))

    df['exhaust_trade_bull'] = (df['exhaust_condition_bull'].shift(periods=1) == True) & (
                df['close'] > df['high'].shift(periods=1))
    df['exhaust_trade_bear'] = (df['exhaust_condition_bear'].shift(periods=1) == True) & (
                df['close'] < df['low'].shift(periods=1))
    return df

def TBBLBG(df):

    df['TB_up'] = ((df['high'] - df['low']) > 2 * df['ATR20']) & (
                df['volume'] > df['volume'].shift(periods=1)) & (
                                   (df['high'] - df['close']) < 0.33 * (df['high'] - df['low']))
    df['TB_down'] = ((df['high'] - df['low']) > 2 * df['ATR20']) & (
                df['volume'] > df['volume'].shift(periods=1)) & (
                                     (df['close'] - df['low']) < 0.33 * (df['high'] - df['low']))

    df['BL_up'] = (df['low'] > df['close'].shift(periods=1)) & (
                df['low'] < df['high'].shift(periods=1)) & (
                                   (df['volume'] > df['volume'].shift(periods=1)) | (
                                       df['volume'] > df['VMA30']))
    df['BL_down'] = (df['high'] < df['close'].shift(periods=1)) & (
                df['high'] > df['low'].shift(periods=1)) & (
                                     (df['volume'] > df['volume'].shift(periods=1)) | (
                                         df['volume'] > df['VMA30']))

    df['BG_up'] = (df['low'] > df['high'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['VMA30']))
    df['BG_down'] = (df['high'] < df['low'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['VMA30']))

    df['TBBLBG_up'] = df['TB_up'] | df['BL_up'] | df['BG_up']
    df['TBBLBG_down'] = df['TB_down'] | df['BL_down'] | df['BG_down']

    return df

def TBBLBG_num(df):

    df.loc[df['TBBLBG_up'] == False, 'TBBLBG_up_num'] = 0
    df.loc[df['TBBLBG_up'] == True, 'TBBLBG_up_num'] = 1

    df.loc[df['TBBLBG_down'] == False, 'TBBLBG_down_num'] = 0
    df.loc[df['TBBLBG_down'] == True, 'TBBLBG_down_num'] = 1

    return df

def runaway(df):

    df['runaway_up_521'] = df['TBBLBG_up_num'].rolling(21).sum()
    df['runaway_down_521'] = df['TBBLBG_down_num'].rolling(21).sum()

    df['runaway_up_1030'] = df['TBBLBG_up_num'].rolling(30).sum()
    df['runaway_down_1030'] = df['TBBLBG_down_num'].rolling(30).sum()

    df['runaway_up_0205'] = df['TBBLBG_up_num'].rolling(5).sum()
    df['runaway_down_0205'] = df['TBBLBG_down_num'].rolling(5).sum()

    return df

def get_key_indicators(tmp_df):

    tmp_df = calculate_moving_average(tmp_df)

    tmp_df = calculate_ATR(tmp_df)

    tmp_df = BB_band(tmp_df)

    tmp_df = KC_band(tmp_df)

    tmp_df = volume_MA(tmp_df)

    tmp_df = yearly_high(tmp_df)

    tmp_df = yearly_low(tmp_df)

    tmp_df = adx(tmp_df)

    tmp_df = rsi(tmp_df)

    tmp_df = macd(tmp_df)

    tmp_df = stoch(tmp_df)

    tmp_df = OBV(tmp_df)

    tmp_df = reversals(tmp_df)

    tmp_df = key_reversals(tmp_df)

    tmp_df = trueRange(tmp_df)

    tmp_df = cpr(tmp_df)

    tmp_df = cgc(tmp_df)

    tmp_df = outside_bar(tmp_df)

    tmp_df = inside_bar(tmp_df)

    tmp_df = signal_bar(tmp_df)

    tmp_df = noRange(tmp_df)

    tmp_df = tr3(tmp_df)

    tmp_df = stalling_bear(tmp_df)

    tmp_df = LRHCHV(tmp_df)

    tmp_df = LVLR(tmp_df)

    tmp_df = exhaust(tmp_df)

    tmp_df = TBBLBG(tmp_df)

    tmp_df = TBBLBG_num(tmp_df)

    tmp_df = runaway(tmp_df)

    return(tmp_df)

def copy_from_stringio(conn, dff, table):
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

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port


    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    allprice_df = get_allprice_data(con)
    allprice_df['date'] = allprice_df.timestamp.apply(lambda x: x.date())

    number_of_tickers = len(allprice_df['Ticker'].unique())

    #get the list of unique tickers
    unique_tickers = allprice_df['Ticker'].unique()
    #unique_tickers = ['AAPL', 'AMZN', 'TSLA']
    i= 0

    tot_df = pd.DataFrame()

    for t in unique_tickers:
        i=i+1

        if i == 100 or i == 1000 or i == 2000 or i == 3000 or i == 4000 or i == 5000 or i == 6000 or i == 7000 \
                or i == 8000 or i == 9000 or i == 10000:
            print("processing ticker %s" % t)
            print("processing ticker %s out of %s" % (i, number_of_tickers))

        tmp_df = allprice_df.loc[allprice_df['Ticker']==t]
        tmp_df = tmp_df.sort_values(by="date", ascending=True)

        tmp_df = tmp_df.reset_index(drop=True)

        tmp_df = tmp_df[["Ticker", "date", "timestamp", "high", "low", "open", "close", "volume", "openinterest"]]

        # get last active date
        # last_active_date = tmp_df.date.max()

        tmp_df = get_key_indicators(tmp_df)

        tot_df = pd.concat([tot_df,tmp_df], ignore_index=True)


    # write df to local csv file
    path_str = r"D:\data\db\key_indicators_population_allTickers.csv"

    tot_df.to_csv(path_str, index=False)

    # copy from dataframe to table
    copy_from_stringio(con, tot_df, "key_indicators_alltickers")

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          (end - start) * 10 ** 3, "ms")

    























