import pandas as pd
import psycopg2
import sys
import talib as ta
from io import StringIO
import time
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

   
### First level data

def get_allprice_data(con):
    cursor = con.cursor()
    select_query = "select * from usstockseod"
    cursor.execute(select_query)
    valid_prices = cursor.fetchall()

    df = pd.DataFrame(valid_prices,
                      columns=['ticker', 'timestamp', 'high', 'low', 'open', 'close', 'volume', 'openinterest'])

    return df


### Second level data
def calculate_moving_average(df, newcols2):
    new_entries = {
        'sma10': ta.SMA(df["close"], timeperiod=10),
        'sma50': ta.SMA(df["close"], timeperiod=50),
        'sma200': ta.SMA(df["close"], timeperiod=200),
        'ema20': ta.EMA(df["close"], timeperiod=20)
    }

    newcols2.update(new_entries)

    return newcols2


def calculate_ATR(df, newcols2):
    new_entries = {
        "atr10": ta.ATR(df["high"], df["low"], df["close"], timeperiod=10),
        "atr14": ta.ATR(df["high"], df["low"], df["close"], timeperiod=14),
        "atr20": ta.ATR(df["high"], df["low"], df["close"], timeperiod=20)
    }

    newcols2.update(new_entries)

    return newcols2


def volume_MA(df, newcols2):
    new_entries = {
        "vma30": ta.MA(df["volume"], timeperiod=30),
        "vma50": ta.MA(df["volume"], timeperiod=50),
        "vma63": ta.MA(df["volume"], timeperiod=63)
    }

    newcols2.update(new_entries)

    return newcols2


def trueRange(df, newcols2):
    new_entries = {
        "tr": ta.TRANGE(df["high"], df["low"], df["close"])
    }

    newcols2.update(new_entries)

    return newcols2


def BB_band(df, newcols2):
    new_entries = {
        "bb_upper": ta.BBANDS(df["close"], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[0],
        "bb_lower": ta.BBANDS(df["close"], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[1]
    }

    newcols2.update(new_entries)

    return newcols2


def yearly_high(df, newcols2):
    new_entries = {
        "w52high": ta.MAX(df["high"], timeperiod=252)
    }

    newcols2.update(new_entries)

    return newcols2


def yearly_low(df, newcols2):
    new_entries = {
        "w52low": ta.MIN(df["low"], timeperiod=252)
    }

    newcols2.update(new_entries)

    return newcols2


def adx(df, newcols2):
    new_entries = {
        "adx14": ta.ADX(df["high"], df["low"], df["close"], timeperiod=14),
        "adx20": ta.ADX(df["high"], df["low"], df["close"], timeperiod=20)
    }

    newcols2.update(new_entries)

    return newcols2


def macd(df, newcols2):
    macd, macdsignal, macdhist = ta.MACD(df["close"], fastperiod=12, slowperiod=26, signalperiod=9)

    new_entries = {
        "macd": macd,
        "macdsignal": macdsignal,
        "macdhist": macdhist
    }

    newcols2.update(new_entries)

    return newcols2


def rsi(df, newcols2):
    new_entries = {
        "rsi14": ta.RSI(df["close"], timeperiod=14),
        "rsi20": ta.RSI(df["close"], timeperiod=20)
    }

    newcols2.update(new_entries)

    return newcols2


def stoch(df, newcols2):
    new_entries = {
        "stoch14": ta.STOCH(df["high"], df["low"], df["close"], fastk_period=14, slowk_period=3, slowd_period=3)[0],
        "stoch20": ta.STOCH(df["high"], df["low"], df["close"], fastk_period=20, slowk_period=3, slowd_period=3)[0]
    }

    newcols2.update(new_entries)

    return newcols2


def OBV(df, newcols2):
    new_entries = {"obv": ta.OBV(df["close"], df["volume"])}

    newcols2.update(new_entries)

    return newcols2


def inside_bar(df, newcols2):
    new_entries = {
        'inside_bar': (df['low'] > df['low'].shift(periods=1)) & (
                df['high'] < df['high'].shift(periods=1))
    }

    newcols2.update(new_entries)

    return newcols2


def signal_bar(df, newcols2):
    new_entries = {
        'signal_bar_bull': (df['open'] < df['low'].shift(periods=1)) & (
                df['close'] > df['open']) & ((df['high'] - df['close']) < 0.2 * (
                df['high'] - df['low'])),
        'signal_bar_bear': (df['open'] > df['high'].shift(periods=1)) & (
                df['close'] < df['open']) & ((df['close'] - df['low']) < 0.2 * (
                df['high'] - df['low']))
    }

    newcols2.update(new_entries)

    return newcols2


def mBR(df, newcols2):
    new_entries = {
        'br2': np.maximum(df['high'], df['high'].shift(periods=1)) - np.minimum(df['low'], df['low'].shift(periods=1)),
        'br3': np.maximum(df['high'], df['high'].shift(periods=1), df['high'].shift(periods=2)) - np.minimum(df['low'],
                                                                                                             df[
                                                                                                                 'low'].shift(
                                                                                                                 periods=1),
                                                                                                             df[
                                                                                                                 'low'].shift(
                                                                                                                 periods=2)),
        'br4': np.maximum.reduce([df['high'], df['high'].shift(periods=1), df['high'].shift(periods=2),
                                  df['high'].shift(periods=3)]) - np.minimum.reduce(
            [df['low'], df['low'].shift(periods=1), df['low'].shift(periods=2), df['low'].shift(periods=3)]),
        'br8': np.maximum.reduce(
            [df['high'], df['high'].shift(periods=1), df['high'].shift(periods=2), df['high'].shift(periods=3),
             df['high'].shift(periods=4), df['high'].shift(periods=5), df['high'].shift(periods=6),
             df['high'].shift(periods=7)]) \
               - np.minimum.reduce(
            [df['low'], df['low'].shift(periods=1), df['low'].shift(periods=2), df['low'].shift(periods=3),
             df['low'].shift(periods=4), df['low'].shift(periods=5), df['low'].shift(periods=6),
             df['low'].shift(periods=7)]),

    }

    newcols2.update(new_entries)

    return newcols2


def doji(df, newcols2):
    new_entries = {
        'doji': abs(df['close'] - df['open']) <= 0.001 * df['close']
    }

    newcols2.update(new_entries)

    return newcols2


def midRange(df, newcols2):
    new_entries = {
        'mid_range': df['low'] + ((df['high'] - df['low']) / 2)
    }

    newcols2.update(new_entries)

    return newcols2


### Third level data
def KC_band(df, newcols3):
    new_entries = {
        "kcmiddle": df["ema20"]
    }

    newcols3.update(new_entries)

    return newcols3


def reversals(df, newcols3):
    new_entries = {
        'reversal_bull': (df['low'] < df['low'].shift(periods=1)) & (
                df['close'] > df['open']) & ((df['volume'] > df['volume'].shift(periods=1)) | (
                df['volume'] > df['vma30'])),
        'reversal_bear': (df['high'] > df['high'].shift(periods=1)) & (
                df['close'] < df['open']) & ((df['volume'] > df['volume'].shift(periods=1)) | (
                df['volume'] > df['vma30']))
    }

    newcols3.update(new_entries)

    return newcols3


def key_reversals(df, newcols3):
    new_entries = {
        'key_reversal_bull': (df['low'] < df['low'].shift(periods=1)) & (
                df['close'] > df['close'].shift(periods=1)) & (df['close'] > df['open']) & (
                                     (df['volume'] > df['volume'].shift(periods=1)) | (
                                     df['volume'] > df['vma30'])),
        'key_reversal_bear': (df['high'] > df['high'].shift(periods=1)) & (
                df['close'] < df['close'].shift(periods=1)) & (df['close'] < df['open']) & (
                                     (df['volume'] > df['volume'].shift(periods=1)) | (
                                     df['volume'] > df['vma30']))
    }

    newcols3.update(new_entries)

    return newcols3


def cpr(df, newcols3):
    new_entries = {
        'cpr_bull': (df['close'] > df['high'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30'])),
        'cpr_bear': (df['close'] < df['low'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30']))
    }

    newcols3.update(new_entries)

    return newcols3


def cgc(df, newcols3):
    new_entries = {
        'cgc_bull': (df['close'] > df['high'].shift(periods=1)) & (
                df['open'] > df['close'].shift(periods=1)) & (
                            df['low'] > df['close'].shift(periods=1)) & (
                            (df['volume'] > df['volume'].shift(periods=1)) | (
                            df['volume'] > df['vma30'])),
        'cgc_bear': (df['close'] < df['low'].shift(periods=1)) & (
                df['open'] < df['close'].shift(periods=1)) & (
                            df['high'] < df['close'].shift(periods=1)) & (
                            (df['volume'] > df['volume'].shift(periods=1)) | (
                            df['volume'] > df['vma30']))
    }

    newcols3.update(new_entries)

    return newcols3


def outside_bar(df, newcols3):
    new_entries = {
        'outside_bar_up': (df['close'] > df['open']) & (
                df['low'] < df['low'].shift(periods=1)) & (
                                  df['high'] > df['high'].shift(periods=1)) & (
                                  (df['high'] - df['close']) < 0.33 * (
                                  df['high'] - df['low'])) & (
                                  (df['volume'] > df['volume'].shift(periods=1)) | (
                                  df['volume'] > df['vma30'])),
        'outside_bar_down': (df['close'] < df['open']) & (
                df['low'] < df['low'].shift(periods=1)) & (
                                    df['high'] > df['high'].shift(periods=1)) & (
                                    (df['close'] - df['low']) < 0.33 * (
                                    df['high'] - df['low'])) & (
                                    (df['volume'] > df['volume'].shift(periods=1)) | (
                                    df['volume'] > df['vma30']))
    }

    newcols3.update(new_entries)

    return newcols3


def noRange(df, newcols3):
    new_entries = {
        'nr7': (df['tr'] < df['tr'].shift(periods=1)) & (df['tr'] < df['tr'].shift(periods=2)) & (
                df['tr'] < df['tr'].shift(periods=3)) & (df['tr'] < df['tr'].shift(periods=4)) & (
                       df['tr'] < df['tr'].shift(periods=5)) & (df['tr'] < df['tr'].shift(periods=6)),

        'nr6': (df['tr'] < df['tr'].shift(periods=1)) & (df['tr'] < df['tr'].shift(periods=2)) & (
                df['tr'] < df['tr'].shift(periods=3)) & (df['tr'] < df['tr'].shift(periods=4)) & (
                       df['tr'] < df['tr'].shift(periods=5)),

        'nr5': (df['tr'] < df['tr'].shift(periods=1)) & (df['tr'] < df['tr'].shift(periods=2)) & (
                df['tr'] < df['tr'].shift(periods=3)) & (df['tr'] < df['tr'].shift(periods=4)),

        'nr4': (df['tr'] < df['tr'].shift(periods=1)) & (df['tr'] < df['tr'].shift(periods=2)) & (
                df['tr'] < df['tr'].shift(periods=3)),

        'nr3': (df['tr'] < df['tr'].shift(periods=1)) & (df['tr'] < df['tr'].shift(periods=2)),

        'nr2': (df['tr'] < df['tr'].shift(periods=1)),
    }

    newcols3.update(new_entries)

    return newcols3


def Hooks(df, newcols3):
    new_entries = {
        'bearhook': (df['open'] < df['low'].shift(periods=1)) & (df['close'] > df['close'].shift(periods=1)) & (
                    df['tr'] < df['tr'].shift(periods=1)),

        'bullhook': (df['open'] > df['high'].shift(periods=1)) & (df['close'] < df['close'].shift(periods=1)) & (
                    df['tr'] < df['tr'].shift(periods=1))
    }

    newcols3.update(new_entries)

    return newcols3


def wideSpread(df, newcols3):
    new_entries = {
        'ws7': (df['tr'] > df['tr'].shift(periods=1)) & (df['tr'] > df['tr'].shift(periods=2)) & (
                df['tr'] > df['tr'].shift(periods=3)) & (df['tr'] > df['tr'].shift(periods=4)) & (
                       df['tr'] > df['tr'].shift(periods=5)) & (df['tr'] > df['tr'].shift(periods=6)),

        'ws6': (df['tr'] > df['tr'].shift(periods=1)) & (df['tr'] > df['tr'].shift(periods=2)) & (
                df['tr'] > df['tr'].shift(periods=3)) & (df['tr'] > df['tr'].shift(periods=4)) & (
                       df['tr'] > df['tr'].shift(periods=5)),

        'ws5': (df['tr'] > df['tr'].shift(periods=1)) & (df['tr'] > df['tr'].shift(periods=2)) & (
                df['tr'] > df['tr'].shift(periods=3)) & (df['tr'] > df['tr'].shift(periods=4)),

        'ws4': (df['tr'] > df['tr'].shift(periods=1)) & (df['tr'] > df['tr'].shift(periods=2)) & (
                df['tr'] > df['tr'].shift(periods=3)),

        'ws3': (df['tr'] > df['tr'].shift(periods=1)) & (df['tr'] > df['tr'].shift(periods=2)),

        'ws2': (df['tr'] > df['tr'].shift(periods=1))
    }

    newcols3.update(new_entries)

    return newcols3


def tr3(df, newcols3):
    new_entries = {
        'tr3': (df['tr'] < 0.5 * df['atr14']) & (
                df['tr'].shift(periods=1) < 0.5 * df['atr14'].shift(periods=1)) & (
                       df['tr'].shift(periods=2) < 0.5 * df['atr14'].shift(periods=2))
    }

    newcols3.update(new_entries)

    return newcols3


def stalling_bear(df, newcols3):
    new_entries = {
        'stalling_bear': (df['volume'] > df['volume'].shift(periods=1)) & (
                df['close'] > df['close'].shift(periods=1)) & ((df['high'] - df['close']) > 0.5 * (
                df['high'] - df['low'])) & ((df['high'] - df['open']) < 0.2 * (
                df['high'] - df['low']))
    }

    newcols3.update(new_entries)

    return newcols3


def LRHCHV(df, newcols3):
    new_entries = {
        'lrhchv': (df['tr'] > 1.75 * df['atr14']) & (
                (df['high'] - df['close']) < 0.2 * (df['high'] - df['low'])) & (
                          df['close'] > df['open']) & (
                          (df['volume'] > df['volume'].shift(periods=1)) & (
                          df['volume'] > df['vma30'])),
        'lrlchv': (df['tr'] > 1.75 * df['atr14']) & (
                (df['close'] - df['low']) < 0.2 * (df['high'] - df['low'])) & (
                          df['close'] < df['open']) & (
                          (df['volume'] > df['volume'].shift(periods=1)) & (
                          df['volume'] > df['vma30']))
    }

    newcols3.update(new_entries)

    return newcols3


def LVLR(df, newcols3):
    new_entries = {
        'lv_day': df['volume'] < 0.5 * df['vma30'],
        'lr_day': df['tr'] < 0.6 * df['atr14']
    }

    newcols3.update(new_entries)

    return newcols3


def VLV(df, newcols3):
    new_entries = {
        'vlv_day': df['volume'] < 0.4 * df['vma30']
    }

    newcols3.update(new_entries)

    return newcols3


def exhaust(df, newcols3):
    new_entries = {
        'exhaust_bar_up': (df['tr'] > 2 * df['atr14']) & (
                df['volume'] > 1.5 * df['atr14']) & (df['close'] > df['open']),
        'exhaust_bar_down': (df['tr'] > 2 * df['atr14']) & (
                df['volume'] > 1.5 * df['atr14']) & (df['close'] < df['open']),
    }

    newcols3.update(new_entries)

    return newcols3


def tbblbg(df, newcols3):
    new_entries = {
        'tb_up': ((df['high'] - df['low']) > 2 * df['atr20']) & (
                df['volume'] > df['volume'].shift(periods=1)) & (
                         (df['high'] - df['close']) < 0.33 * (df['high'] - df['low'])),
        'tb_down': ((df['high'] - df['low']) > 2 * df['atr20']) & (
                df['volume'] > df['volume'].shift(periods=1)) & (
                           (df['close'] - df['low']) < 0.33 * (df['high'] - df['low'])),

        'bl_up': (df['low'] > df['close'].shift(periods=1)) & (
                df['low'] < df['high'].shift(periods=1)) & (
                         (df['volume'] > df['volume'].shift(periods=1)) | (
                         df['volume'] > df['vma30'])),
        'bl_down': (df['high'] < df['close'].shift(periods=1)) & (
                df['high'] > df['low'].shift(periods=1)) & (
                           (df['volume'] > df['volume'].shift(periods=1)) | (
                           df['volume'] > df['vma30'])),

        'bg_up': (df['low'] > df['high'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30'])),
        'bg_down': (df['high'] < df['low'].shift(periods=1)) & (
                (df['volume'] > df['volume'].shift(periods=1)) | (df['volume'] > df['vma30'])),

    }

    newcols3.update(new_entries)

    return newcols3


def rBRN(df, newcols3):
    new_entries = {
        'rbr2': df['br2'].rolling(window=20).min(),
        'rbr3': df['br3'].rolling(window=20).min(),
        'rbr4': df['br4'].rolling(window=30).min(),
        'rbr8': df['br8'].rolling(window=40).min()
    }

    newcols3.update(new_entries)

    return newcols3


### Stage 4 level data

def KC_band_extra(df, newcols4):
    new_entries = {
        "kcupper": df["kcmiddle"] + 2 * df["atr10"],
        "kclower": df["kcmiddle"] - 2 * df["atr10"]
    }

    newcols4.update(new_entries)

    return newcols4


def tbblbg_cond(df, newcols4):
    new_entries = {
        'tbblbg_up': df['tb_up'] | df['bl_up'] | df['bg_up'],
        'tbblbg_down': df['tb_down'] | df['bl_down'] | df['bg_down']
    }

    newcols4.update(new_entries)

    return newcols4


def exhaust_cond(df, newcols4):
    new_entries = {
        'exhaust_condition_bull': (df['exhaust_bar_up'].shift(periods=1) == True) & (
                df['close'] > df['close'].shift(periods=1)),
        'exhaust_condition_bear': (df['exhaust_bar_down'].shift(periods=1) == True) & (
                df['close'] < df['close'].shift(periods=1)),
    }

    newcols4.update(new_entries)

    return newcols4


def mBNR_cond(df, newcols4):
    new_entries = {
        'twobnr': df['br2'] <= df['rbr2'],
        'threebnr': df['br3'] <= df['rbr3'],
        'fourbnr': df['br4'] <= df['rbr4'],
        'eightbnr': df['br8'] <= df['rbr8']
    }

    newcols4.update(new_entries)

    return newcols4


### Stage 5 level data

def tbblbg_num(df):
    df.loc[df['tbblbg_up'] == False, 'tbblbg_up_num'] = 0
    df.loc[df['tbblbg_up'] == True, 'tbblbg_up_num'] = 1

    df.loc[df['tbblbg_down'] == False, 'tbblbg_down_num'] = 0
    df.loc[df['tbblbg_down'] == True, 'tbblbg_down_num'] = 1

    return df


def exhaust_final(df, newcols5):
    new_entries = {
        'exhaust_trade_bull': (df['exhaust_condition_bull'].shift(periods=1) == True) & (
                df['close'] > df['high'].shift(periods=1)),
        'exhaust_trade_bear': (df['exhaust_condition_bear'].shift(periods=1) == True) & (
                df['close'] < df['low'].shift(periods=1))
    }

    newcols5.update(new_entries)

    return newcols5


def runaway(df, newcols5):
    new_entries = {
        'runaway_up_521': df['tbblbg_up_num'].rolling(21).sum(),
        'runaway_down_521': df['tbblbg_down_num'].rolling(21).sum(),

        'runaway_up_1030': df['tbblbg_up_num'].rolling(30).sum(),
        'runaway_down_1030': df['tbblbg_down_num'].rolling(30).sum(),

        'runaway_up_0205': df['tbblbg_up_num'].rolling(5).sum(),
        'runaway_down_0205': df['tbblbg_down_num'].rolling(5).sum()
    }

    newcols5.update(new_entries)

    return newcols5


# First extra indicators - trend day, future performances

# extra1, Level1

def td_metric1(df, newcols6):
    tdm1a = ((df['close'] - df['low']) / (df['high'] - df['low'])) >= 0.85
    tdm1b = ((df['high'] - df['close']) / (df['high'] - df['low'])) >= 0.85

    new_entries = {
        'tdm1': tdm1a | tdm1b
    }

    newcols6.update(new_entries)

    return newcols6


def td_metric2(df, newcols6):
    tdm2a = (df['close'] - df['low']) >= 0.75 * df['atr14']
    tdm2b = (df['high'] - df['close']) >= 0.75 * df['atr14']

    new_entries = {
        'tdm2': tdm2a | tdm2b
    }

    newcols6.update(new_entries)

    return newcols6


def td_metric3(df, newcols6):
    tdm3a = ((df['close'] - df['open']) / df['open']) * 100 >= 1.5
    tdm3b = ((df['open'] - df['close']) / df['open']) * 100 >= 1.5

    new_entries = {
        'tdm3': tdm3a | tdm3b
    }

    newcols6.update(new_entries)

    return newcols6


def return_1day(df, newcols6):
    new_entries = {
        'return_1day': (df['close'].shift(-1) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_2days(df, newcols6):
    new_entries = {
        'return_2days': (df['close'].shift(-2) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_3days(df, newcols6):
    new_entries = {
        'return_3days': (df['close'].shift(-3) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_4days(df, newcols6):
    new_entries = {
        'return_4days': (df['close'].shift(-4) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_1week(df, newcols6):
    new_entries = {
        'return_1week': (df['close'].shift(-5) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_2weeks(df, newcols6):
    new_entries = {
        'return_2weeks': (df['close'].shift(-10) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_3weeks(df, newcols6):
    new_entries = {
        'return_3weeks': (df['close'].shift(-15) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_1month(df, newcols6):
    new_entries = {
        'return_1month': (df['close'].shift(-21) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_2months(df, newcols6):
    new_entries = {
        'return_2months': (df['close'].shift(-42) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_1quarter(df, newcols6):
    new_entries = {
        'return_1quarter': (df['close'].shift(-63) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_2quarters(df, newcols6):
    new_entries = {
        'return_2quarters': (df['close'].shift(-126) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_3quarters(df, newcols6):
    new_entries = {
        'return_3quarters': (df['close'].shift(-189) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


def return_1year(df, newcols6):
    new_entries = {
        'return_1year': (df['close'].shift(-252) - df['close']) / df['close']
    }

    newcols6.update(new_entries)

    return newcols6


# extra1, Level2

def trend_day(df, newcols7):
    new_entries = {
        'trend_day': df['tdm1'] & df['tdm2'] & df['tdm3']
    }

    newcols7.update(new_entries)

    return newcols7

def likely_trend_day(df, newcols7):
    new_entries = {
        'likely_trend_day': df['tdm1'] | df['tdm2'] | df['tdm3']
    }

    newcols7.update(new_entries)

    return newcols7

# extra1, level3

def future_trend_day(df, newcols8):

    new_entries = {
        'ftd': df['trend_day'].shift(-1),
        'fltd': df['likely_trend_day'].shift(-1),
        'ftdm1': df['tdm1'].shift(-1),
        'ftdm2': df['tdm2'].shift(-1),
        'ftdm3': df['tdm3'].shift(-1)
    }

    newcols8.update(new_entries)

    return newcols8

def get_key_indicators(tmp_df):
    ###Level 2 data
    new_columns2 = {}

    new_columns2 = calculate_moving_average(tmp_df, new_columns2)

    new_columns2 = calculate_ATR(tmp_df, new_columns2)

    new_columns2 = volume_MA(tmp_df, new_columns2)

    new_columns2 = trueRange(tmp_df, new_columns2)

    new_columns2 = BB_band(tmp_df, new_columns2)

    new_columns2 = yearly_high(tmp_df, new_columns2)

    new_columns2 = yearly_low(tmp_df, new_columns2)

    new_columns2 = adx(tmp_df, new_columns2)

    new_columns2 = macd(tmp_df, new_columns2)

    new_columns2 = rsi(tmp_df, new_columns2)

    new_columns2 = stoch(tmp_df, new_columns2)

    new_columns2 = OBV(tmp_df, new_columns2)

    new_columns2 = inside_bar(tmp_df, new_columns2)

    new_columns2 = signal_bar(tmp_df, new_columns2)

    new_columns2 = mBR(tmp_df, new_columns2)

    new_columns2 = doji(tmp_df, new_columns2)

    new_columns2 = midRange(tmp_df, new_columns2)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns2)], axis=1)

    ###Level 3 data

    new_columns3 = {}

    new_columns3 = KC_band(tmp_df, new_columns3)

    new_columns3 = reversals(tmp_df, new_columns3)

    new_columns3 = key_reversals(tmp_df, new_columns3)

    new_columns3 = cpr(tmp_df, new_columns3)

    new_columns3 = cgc(tmp_df, new_columns3)

    new_columns3 = outside_bar(tmp_df, new_columns3)

    new_columns3 = noRange(tmp_df, new_columns3)

    new_columns3 = Hooks(tmp_df, new_columns3)

    new_columns3 = wideSpread(tmp_df, new_columns3)

    new_columns3 = tr3(tmp_df, new_columns3)

    new_columns3 = stalling_bear(tmp_df, new_columns3)

    new_columns3 = LRHCHV(tmp_df, new_columns3)

    new_columns3 = LVLR(tmp_df, new_columns3)

    new_columns3 = VLV(tmp_df, new_columns3)

    new_columns3 = exhaust(tmp_df, new_columns3)

    new_columns3 = tbblbg(tmp_df, new_columns3)

    new_columns3 = rBRN(tmp_df, new_columns3)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns3)], axis=1)

    ###Level 4 data

    new_columns4 = {}

    new_columns4 = KC_band_extra(tmp_df, new_columns4)

    new_columns4 = tbblbg_cond(tmp_df, new_columns4)

    new_columns4 = exhaust_cond(tmp_df, new_columns4)

    new_columns4 = mBNR_cond(tmp_df, new_columns4)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns4)], axis=1)

    ###Level 5 data

    tmp_df = tbblbg_num(tmp_df)

    new_columns5 = {}

    new_columns5 = exhaust_final(tmp_df, new_columns5)

    new_columns5 = runaway(tmp_df, new_columns5)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns5)], axis=1)

    ### extra-1
    ### extra-1, level-1

    new_columns6 = {}

    new_columns6 = td_metric1(tmp_df, new_columns6)

    new_columns6 = td_metric2(tmp_df, new_columns6)

    new_columns6 = td_metric3(tmp_df, new_columns6)

    new_columns6 = return_1day(tmp_df, new_columns6)

    new_columns6 = return_2days(tmp_df, new_columns6)

    new_columns6 = return_3days(tmp_df, new_columns6)

    new_columns6 = return_4days(tmp_df, new_columns6)

    new_columns6 = return_1week(tmp_df, new_columns6)

    new_columns6 = return_2weeks(tmp_df, new_columns6)

    new_columns6 = return_3weeks(tmp_df, new_columns6)

    new_columns6 = return_1month(tmp_df, new_columns6)

    new_columns6 = return_2months(tmp_df, new_columns6)

    new_columns6 = return_1quarter(tmp_df, new_columns6)

    new_columns6 = return_2quarters(tmp_df, new_columns6)

    new_columns6 = return_3quarters(tmp_df, new_columns6)

    new_columns6 = return_1year(tmp_df, new_columns6)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns6)], axis=1)

    ### extra-1, level-2

    new_columns7 = {}

    new_columns7 = trend_day(tmp_df, new_columns7)

    new_columns7 = likely_trend_day(tmp_df, new_columns7)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns7)], axis=1)

    ### extra-1, level-3

    new_columns8 = {}

    new_columns8 = future_trend_day(tmp_df, new_columns8)

    tmp_df = pd.concat([tmp_df, pd.DataFrame(new_columns8)], axis=1)

    return (tmp_df)


def create_table(con, tot_df):
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT'
    }

    columns = []
    for col, dtype in tot_df.dtypes.items():
        sql_dtype = dtype_mapping[str(dtype)]
        columns.append(f"{col} {sql_dtype}")

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS key_indicators_alltickers (
            {', '.join(columns)}
        );
        """
    try:
        cursor = con.cursor()
        cursor.execute(create_table_query)
        con.commit()
        print("Table created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


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
        problematic_line = buffer.getvalue().split('\n')[0]
        print("Problematic line: %s" % problematic_line)
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

    print("getting all price from all data")

    allprice_df = get_allprice_data(con)
    allprice_df['date'] = allprice_df.timestamp.apply(lambda x: x.date())

    number_of_tickers = len(allprice_df['ticker'].unique())

    #get the list of unique tickers
    unique_tickers = allprice_df['ticker'].unique()
    #unique_tickers = ['SPY', 'AAPL', 'AMZN', 'TSLA']
    i= 0

    tot_df = pd.DataFrame()

    print("getting indicators data")

    for t in unique_tickers:
        i=i+1

        if i == 1 or i == 100 or i == 1000 or i == 2000 or i == 3000 or i == 4000 or i == 5000 or i == 6000 or i == 7000 \
                or i == 8000 or i == 9000 or i == 10000 or i == 11000:
            print("processing ticker %s" % t)
            print("processing ticker %s out of %s" % (i, number_of_tickers))

        tmp_df = allprice_df.loc[allprice_df['ticker']==t]
        tmp_df = tmp_df.sort_values(by="date", ascending=True)

        tmp_df = tmp_df.reset_index(drop=True)

        tmp_df = tmp_df[["ticker", "date", "timestamp", "high", "low", "open", "close", "volume", "openinterest"]]

        # get last active date
        # last_active_date = tmp_df.date.max()
        tmp_df = get_key_indicators(tmp_df)


        tot_df = pd.concat([tot_df,tmp_df], ignore_index=True)

    tot_df = tot_df.fillna(0)


    # write df to local csv file
    path_str = r"D:\data\db\key_indicators_population_allTickers.csv"


    tot_df.to_csv(path_str, index=False)

    #create_table(con, tot_df)

    print("csv uploaded")
    # copy from dataframe to table
    copy_from_stringio(con, tot_df, "key_indicators_alltickers")

    print("Data uploaded in the table")

    con.close()

    # record end time
    end = time.time()


    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")

    sys.exit(0)


    























