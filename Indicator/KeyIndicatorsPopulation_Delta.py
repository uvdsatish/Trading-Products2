import pandas as pd
import psycopg2
from io import StringIO
import sys
import talib as ta
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

def get_valid_dates(con):
    cursor = con.cursor()
    select_query = "select timestamp from valid_trading_dates"
    cursor.execute(select_query)
    valid_dates = cursor.fetchall()

    df = pd.DataFrame(valid_dates, columns=['date'])

    return df


def get_allprice_data(con):
    cursor = con.cursor()
    select_query = "select * from usstockseod_sinceapril2022_view"
    cursor.execute(select_query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker', 'timestamp', 'high', 'low', 'open', 'close', 'volume', 'openinterest'])

    return df

def get_current_data(con):
    cursor = con.cursor()
    select_query = "select * from key_indicators_alltickers_sinceapril2022_view"
    cursor.execute(select_query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
            columns = ['ticker', 'date', 'timestamp', 'high', 'low', 'open', 'close',
               'volume', 'openinterest', 'sma10', 'sma50', 'sma200', 'ema20',
               'atr10', 'atr14', 'atr20', 'bb_upper', 'bb_lower', 'kc_middle', 'kc_upper',
               'kc_lower', 'vma30', 'vma50', 'vma63', 'w52high', 'w52low', 'adx14', 'adx20',
               'rsi14', 'rsi20', 'macd', 'macdsignal', 'macdhist', 'stoch14', 'stoch20', 'obv',
               'reversal_bull', 'reversal_bear', 'key_reversal_bull', 'key_reversal_bear',
               'tradingrange', 'cpr_bull', 'cpr_bear', 'cgc_bull', 'cgc_bear', 'outside_bar_up',
               'outside_bar_down', 'inside_bar', 'signal_bar_bull', 'signal_bar_bear',
               'nr7', 'tr3', 'stalling_bear', 'lrhchv', 'lrlchv', 'lv_day', 'lr_day', 'exhaust_bar_up',
               'exhaust_bar_down', 'exhaust_condition_bull', 'exhaust_condition_bear',
               'exhaust_trade_bull', 'exhaust_trade_bear', 'tb_up', 'tb_down', 'bl_up', 'bl_down',
               'bg_up', 'bg_down', 'tbblbg_up', 'tbblbg_down', 'tbblbg_up_num', 'tbblbg_down_num',
               'runaway_up_521', 'runaway_down_521', 'runaway_up_1030', 'runaway_down_1030',
               'runaway_up_0205', 'runaway_down_0205'])


    return df

def calculate_moving_average(df):
    df["sma10"] = ta.SMA(df["close"], timeperiod=10)
    df["sma50"] = ta.SMA(df["close"], timeperiod=50)
    df["sma200"] = ta.SMA(df["close"], timeperiod=200)
    df["ema20"] = ta.EMA(df["close"], timeperiod=20)

    return df

def calculate_ATR(df):
    df["atr10"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=10)
    df["atr14"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=14)
    df["atr20"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=20)

    return df

def BB_band(df):

    df["bb_upper"] = ta.BBANDS(df["close"], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[0]
    df["bb_lower"] = ta.BBANDS(df["close"], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[1]

    return df

def KC_band(df):

    df["kc_middle"] = df["ema20"]
    df["kc_upper"]  = df["kc_middle"] + 2*df["atr10"]
    df["kc_lower"] = df["kc_middle"] - 2*df["atr10"]

    return df

def volume_MA(df):

    df["vma30"] = ta.MA(df["volume"], timeperiod=30)
    df["vma50"] = ta.MA(df["volume"], timeperiod=50)
    df["vma63"] = ta.MA(df["volume"], timeperiod=63)

    return df

def yearly_high(df):

    df["w52high"] = ta.MAX(df["high"], timeperiod=252)

    return df

def yearly_low(df):

    df["w52low"] = ta.MIN(df["low"], timeperiod=252)

    return df

def adx(df):

    df["adx14"] = ta.ADX(df["high"], df["low"], df["close"], timeperiod=14)
    df["adx20"] = ta.ADX(df["high"], df["low"], df["close"], timeperiod=20)

    return df

def rsi(df):

    df["rsi14"] = ta.RSI(df["close"], timeperiod=14)
    df["rsi20"] = ta.RSI(df["close"], timeperiod=20)

    return df

def macd(df):

    df["macd"], df["macdsignal"], df["macdhist"] = ta.MACD(df["close"], fastperiod=12, slowperiod=26, signalperiod=9)

    return df


def stoch(df):

    df["stoch14"] = ta.STOCH(df["high"], df["low"], df["close"], fastk_period=14, slowk_period=3, slowd_period=3)[0]
    df["stoch20"] = ta.STOCH(df["high"], df["low"], df["close"], fastk_period=20, slowk_period=3, slowd_period=3)[0]

    return df

def OBV(df):

    df["obv"] = ta.OBV(df["close"], df["volume"])

    return df

def reversals(df):

    df['reversal_bull'] = (df['low'] < df['low'].shift(periods=1)) & (
                df['close'] > df['open']) & ((df['volume'] > df['volume'].shift(periods=1)) | (
                df['volume'] > df['vma30']))
    df['reversal_bear'] = (df['high'] > df['high'].shift(periods=1)) & (
                df['close'] < df['open']) & ((df['volume'] > df['volume'].shift(periods=1)) | (
                df['volume'] > df['vma30']))

    return df

def key_reversals(df):

    df['key_reversal_bull'] = (df['low'] < df['low'].shift(periods=1)) & (
                df['close'] > df['close'].shift(periods=1)) & (df['close'] > df['open']) & (
                                               (df['volume'] > df['volume'].shift(periods=1)) | (
                                                   df['volume'] > df['vma30']))
    df['key_reversal_bear'] = (df['high'] > df['high'].shift(periods=1)) & (
                df['close'] < df['close'].shift(periods=1)) & (df['close'] < df['open']) & (
                                               (df['volume'] > df['volume'].shift(periods=1)) | (
                                                   df['volume'] > df['vma30']))

    return df

def trueRange(df):
    #supposedly true range

    df["tradingrange"] = ta.TRANGE(df["high"], df["low"], df["close"])

    return df

def cpr(df):

    df['cpr_bull'] = (df['close'] > df['high'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30']))
    df['cpr_bear'] = (df['close'] < df['low'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30']))

    return df

def cgc(df):

    df['cgc_bull'] = (df['close'] > df['high'].shift(periods=1)) & (
                df['open'] > df['close'].shift(periods=1)) & (
                                      df['low'] > df['close'].shift(periods=1)) & (
                                      (df['volume'] > df['volume'].shift(periods=1)) | (
                                          df['volume'] > df['vma30']))
    df['cgc_bear'] = (df['close'] < df['low'].shift(periods=1)) & (
                df['open'] < df['close'].shift(periods=1)) & (
                                      df['high'] < df['close'].shift(periods=1)) & (
                                      (df['volume'] > df['volume'].shift(periods=1)) | (
                                          df['volume'] > df['vma30']))
    return df

def outside_bar(df):

    df['outside_bar_up'] = (df['close'] > df['open']) & (
                df['low'] < df['low'].shift(periods=1)) & (
                                            df['high'] > df['high'].shift(periods=1)) & (
                                            (df['high'] - df['close']) < 0.33 * (
                                                df['high'] - df['low'])) & (
                                            (df['volume'] > df['volume'].shift(periods=1)) | (
                                                df['volume'] > df['vma30']))
    df['outside_bar_down'] = (df['close'] < df['open']) & (
                df['low'] < df['low'].shift(periods=1)) & (
                                              df['high'] > df['high'].shift(periods=1)) & (
                                              (df['close'] - df['low']) < 0.33 * (
                                                  df['high'] - df['low'])) & (
                                              (df['volume'] > df['volume'].shift(periods=1)) | (
                                                  df['volume'] > df['vma30']))
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

    df['nr7'] = (df['tradingrange'] < df['tradingrange'].shift(periods=1)) & (df['tradingrange'] < df['tradingrange'].shift(periods=2)) & (
                df['tradingrange'] < df['tradingrange'].shift(periods=3)) & (df['tradingrange'] < df['tradingrange'].shift(periods=4)) & (
                            df['tradingrange'] < df['tradingrange'].shift(periods=5)) & (df['tradingrange'] < df['tradingrange'].shift(periods=6)) & (
                            df['tradingrange'] < df['tradingrange'].shift(periods=7))

    return df

def tr3(df):

    df['tr3'] = (df['tradingrange'] < 0.5 * df['atr14']) & (
                df['tradingrange'].shift(periods=1) < 0.5 * df['atr14'].shift(periods=1)) & (
                                 df['tradingrange'].shift(periods=2) < 0.5 * df['atr14'].shift(periods=2))

    return df

def stalling_bear(df):

    df['stalling_bear'] = (df['volume'] > df['volume'].shift(periods=1)) & (
                df['close'] > df['close'].shift(periods=1)) & ((df['high'] - df['close']) > 0.5 * (
                df['high'] - df['low'])) & ((df['high'] - df['open']) < 0.2 * (
                df['high'] - df['low']))

    return df

def LRHCHV(df):

    df['lrhchv'] = (df['tradingrange'] > 1.75 * df['atr14']) & (
                (df['high'] - df['close']) < 0.2 * (df['high'] - df['low'])) & (
                                    df['close'] > df['open']) & (
                                    (df['volume'] > df['volume'].shift(periods=1)) & (
                                        df['volume'] > df['vma30']))
    df['lrlchv'] = (df['tradingrange'] > 1.75 * df['atr14']) & (
                (df['close'] - df['low']) < 0.2 * (df['high'] - df['low'])) & (
                                    df['close'] < df['open']) & (
                                    (df['volume'] > df['volume'].shift(periods=1)) & (
                                        df['volume'] > df['vma30']))

    return df

def LVLR(df):

    df['lv_day'] = df['volume'] < 0.5 * df['vma30']
    df['lr_day'] = df['tradingrange'] < 0.6 * df['atr14']

    return df

def exhaust(df):

    df['exhaust_bar_up'] = (df['tradingrange'] > 2 * df['atr14']) & (
                df['volume'] > 1.5 * df['atr14']) & (df['close'] > df['open'])
    df['exhaust_bar_down'] = (df['tradingrange'] > 2 * df['atr14']) & (
                df['volume'] > 1.5 * df['atr14']) & (df['close'] < df['open'])
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

    df['tb_up'] = ((df['high'] - df['low']) > 2 * df['atr20']) & (
                df['volume'] > df['volume'].shift(periods=1)) & (
                                   (df['high'] - df['close']) < 0.33 * (df['high'] - df['low']))
    df['tb_down'] = ((df['high'] - df['low']) > 2 * df['atr20']) & (
                df['volume'] > df['volume'].shift(periods=1)) & (
                                     (df['close'] - df['low']) < 0.33 * (df['high'] - df['low']))

    df['bl_up'] = (df['low'] > df['close'].shift(periods=1)) & (
                df['low'] < df['high'].shift(periods=1)) & (
                                   (df['volume'] > df['volume'].shift(periods=1)) | (
                                       df['volume'] > df['vma30']))
    df['bl_down'] = (df['high'] < df['close'].shift(periods=1)) & (
                df['high'] > df['low'].shift(periods=1)) & (
                                     (df['volume'] > df['volume'].shift(periods=1)) | (
                                         df['volume'] > df['vma30']))

    df['bg_up'] = (df['low'] > df['high'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30']))
    df['bg_down'] = (df['high'] < df['low'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30']))

    df['tbblbg_up'] = df['tb_up'] | df['bl_up'] | df['bg_up']
    df['tbblbg_down'] = df['tb_down'] | df['bl_down'] | df['bg_down']

    return df

def TBBLBG_num(df):

    df.loc[df['tbblbg_up'] == False, 'tbblbg_up_num'] = 0
    df.loc[df['tbblbg_up'] == True, 'tbblbg_up_num'] = 1

    df.loc[df['tbblbg_down'] == False, 'tbblbg_down_num'] = 0
    df.loc[df['tbblbg_down'] == True, 'tbblbg_down_num'] = 1

    return df

def runaway(df):

    df['runaway_up_521'] = df['tbblbg_up_num'].rolling(21).sum()
    df['runaway_down_521'] = df['tbblbg_down_num'].rolling(21).sum()

    df['runaway_up_1030'] = df['tbblbg_up_num'].rolling(30).sum()
    df['runaway_down_1030'] = df['tbblbg_down_num'].rolling(30).sum()

    df['runaway_up_0205'] = df['tbblbg_up_num'].rolling(5).sum()
    df['runaway_down_0205'] = df['tbblbg_down_num'].rolling(5).sum()

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

    print("Connected - now getting valid dates")
    valid_dates = get_valid_dates(con)
    valid_dates.date = valid_dates["date"].apply(lambda x: x.date())
    valid_dates = valid_dates.sort_values(by="date", ascending=True)
    valid_dates_list = valid_dates['date'].tolist()

    print("got valid dates -- now getting all price data")
    allprice_df = get_allprice_data(con)
    allprice_df['date'] = allprice_df.timestamp.apply(lambda x: x.date())

    number_of_tickers = len(allprice_df['ticker'].unique())

    print("got valid dates -- now getting all current indicators data")
    allref_df = get_current_data(con)

    #get the list of unique tickers
    unique_tickers = allprice_df['ticker'].unique()
    #unique_tickers = ['AAPL', 'AMZN', 'TSLA']
    i= 0

    tot_df = pd.DataFrame()

    for t in unique_tickers:
        i=i+1
        if i == 100 or  i == 1000 or i == 2000 or i == 3000 or i == 4000 or i == 5000 or i == 6000 or i == 7000\
            or i == 8000 or i == 9000 or i == 10000:
             print("processing ticker %s" % t)
             print("processing ticker %s out of %s" % (i , number_of_tickers))

        ref_df = allref_df.loc[allref_df['ticker'] == t]
        ref_df = ref_df.sort_values(by="date", ascending=True)

        ref_df = ref_df.reset_index(drop=True)

        tmp_df = allprice_df.loc[allprice_df['ticker']==t]
        tmp_df = tmp_df.sort_values(by="date", ascending=True)

        tmp_df = tmp_df.reset_index(drop=True)

        tmp_df = tmp_df[["ticker", "date", "timestamp", "high", "low", "open", "close", "volume", "openinterest"]]

        tmp_df = get_key_indicators(tmp_df)

        delta_df=pd.concat([tmp_df, ref_df]).sort_values('date')

        #drop duplicates based on date and ticker

        delta_df=delta_df.drop_duplicates(subset=['ticker','date'], keep=False)

        tot_df = pd.concat([delta_df,tot_df], ignore_index=True)

    tot_df = tot_df.fillna(0)
    # write df to local csv file
    path_str = r"D:\data\db\key_indicators_population_delta6.csv"

    tot_df.to_csv(path_str, index=False)

    #copy from dataframe to table
    copy_from_stringio(con, tot_df, "key_indicators_alltickers")

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3)/60000, "minutes")





















