from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys
import os
import time
import psycopg2
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
### First level data

def get_allprice_data_sinceapril2021(jdbc_url, jdbc_properties):
    try:
        # Read data into Spark DataFrame using JDBC
        allprice_sdf = spark.read.jdbc(url=jdbc_url, table="usstockseod_sinceapril2021_view",
                                       properties=jdbc_properties) \
            .repartition("ticker")  # Repartitioning based on ticker for parallelism

        print("Data loaded successfully into Spark DataFrame using JDBC")
        return allprice_sdf

    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)


def delete_existing_rows():
    try:
        connection = psycopg2.connect(
            database="markets_technicals",
            user="postgres",
            password="root",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()
        delete_query = "DELETE FROM key_indicators_alltickers WHERE date > '2023-03-31'"
        cursor.execute(delete_query)
        connection.commit()
        print(f"{cursor.rowcount} rows deleted successfully.")
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while deleting rows: {error}")
        sys.exit(1)

# Function to calculate the smoothing factor (alpha) for EMA
def calculate_ema_mult(span):
    return 2 / (span + 1)

def calculate_ema_udf(prices, span):
    # Calculate the smoothing factor (alpha)
    alpha = calculate_ema_mult(span)

    # Use Pandas to calculate the EMA over the column
    return prices.ewm(alpha=alpha, adjust=False).mean()

def calculate_atr_udf(prices, span):
    # Calculate the smoothing factor (alpha)
    alpha = (1/span)

    # Use Pandas to calculate the EMA over the column
    return prices.ewm(alpha=alpha, adjust=False).mean()


# Create the UDF with the correct span
def register_ema_udf(span):
    # The UDF will take a Pandas Series (a partitioned chunk of data) and return a Pandas Series
    return F.pandas_udf(lambda prices: calculate_ema_udf(prices, span), DoubleType())



# Function to calculate moving averages including EMA20
def calculate_moving_average(df):
    # Define window specification for calculating simple moving averages
    window_spec_10 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-9, 0)
    window_spec_50 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-49, 0)
    window_spec_200 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-199, 0)

    # Calculate SMA for 10, 50, and 200 periods
    df = df.withColumn('sma10', F.avg('close').over(window_spec_10))
    df = df.withColumn('sma50', F.avg('close').over(window_spec_50))
    df = df.withColumn('sma200', F.avg('close').over(window_spec_200))


    schema = df.schema

    new_schema = StructType(schema.fields + [StructField("ema20", DoubleType(), True)])

    # Apply the UDF by grouping the data by 'ticker' and applying the Pandas UDF on each group
    df = df.groupBy('ticker').applyInPandas(
        lambda pdf: pdf.assign(ema20=calculate_ema_udf(pdf['close'], 20)),
        schema=new_schema
    )

    return df
def calculate_atr_group(pdf):
    pdf['atr10'] = calculate_atr_udf(pdf['tr'], 10)
    pdf['atr14'] = calculate_atr_udf(pdf['tr'], 14)
    pdf['atr20'] = calculate_atr_udf(pdf['tr'], 20)

    return pdf


# Function to calculate ATR for different periods using EMA
def calculate_ATR(df):
    # Calculate True Range (TR) using PySpark functions
    df = df.withColumn(
        'tr',
        F.greatest(
            F.col('high') - F.col('low'),
            F.abs(F.col('high') - F.lag('close', 1).over(Window.partitionBy('ticker').orderBy('timestamp'))),
            F.abs(F.col('low') - F.lag('close', 1).over(Window.partitionBy('ticker').orderBy('timestamp')))
        )
    )

    schema = df.schema

    new_schema = StructType(schema.fields + [
        StructField("atr10", DoubleType(), True),
        StructField("atr14", DoubleType(), True),
        StructField("atr20", DoubleType(), True)
    ])

    # Apply the UDF by grouping the data by 'ticker' and applying the Pandas UDF on each group
    df = df.groupBy('ticker').applyInPandas(calculate_atr_group, schema=new_schema)

    return df

# Function to calculate volume moving averages in PySpark
def volume_MA(df):
    # Define window specifications for different moving averages
    window_spec_30 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-29, 0)  # Last 30 periods
    window_spec_50 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-49, 0)  # Last 50 periods
    window_spec_63 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-62, 0)  # Last 63 periods

    # Calculate moving averages of volume for 30, 50, and 63 periods
    df = df.withColumn('vma30', F.avg('volume').over(window_spec_30))
    df = df.withColumn('vma50', F.avg('volume').over(window_spec_50))
    df = df.withColumn('vma63', F.avg('volume').over(window_spec_63))

    return df


def BB_band(df):
    """
    Calculate Bollinger Bands for time series data.

    Args:
        df: DataFrame with columns: ticker, timestamp, close
        window_size: Period for moving average and standard deviation (default: 20)
        num_std: Number of standard deviations for bands (default: 2)

    Returns:
        DataFrame with additional columns: bb_upper, bb_middle, bb_lower
    """
    window_size = 20
    num_std = 2
    # Window specification for the rolling calculations
    window_spec = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-(window_size - 1), 0)

    # Calculate middle band (Simple Moving Average)
    df = df.withColumn('bb_middle',
                       F.avg('close').over(window_spec))

    # Calculate standard deviation
    df = df.withColumn('bb_std',
                       F.sqrt(
                           F.avg(F.pow(F.col('close') - F.col('bb_middle'), 2)).over(window_spec)
                       ))

    # Calculate upper and lower bands
    df = df.withColumn('bb_upper',
                       F.col('bb_middle') + (F.col('bb_std') * num_std))

    df = df.withColumn('bb_lower',
                       F.col('bb_middle') - (F.col('bb_std') * num_std))

    # Optional: drop intermediate calculation column
    df = df.drop('bb_std', 'bb_middle')

    return df

def yearly_high_low(df):
    # Define window specifications for calculating 52-week high and low
    window_spec_52w = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-251, 0)

    # Calculate 52-week high and low using PySpark functions
    df = df.withColumn('w52high', F.max('high').over(window_spec_52w))
    df = df.withColumn('w52low', F.min('low').over(window_spec_52w))

    return df

def adx(df):
    periods = [14, 20]
    # Build the schema explicitly
    schema_fields = []

    # Add existing fields from df.schema
    for field in df.schema.fields:
        schema_fields.append(field)

    # Add new ADX columns for each period
    for period in periods:
        schema_fields.append(StructField(f'adx{period}', DoubleType(), True))

    schema = StructType(schema_fields)


    df = df.groupBy('ticker').applyInPandas(adx_calc, schema=schema)

    return df

def adx_calc(pdf):

    periods = [14, 20]
    pdf = pdf.sort_values('timestamp')

    # Calculate Up Move and Down Move
    pdf['up_move'] = pdf['high'] - pdf['high'].shift(1)
    pdf['down_move'] = pdf['low'].shift(1) - pdf['low']

    # Calculate +DM and -DM
    pdf['+dm'] = np.where(
        (pdf['up_move'] > pdf['down_move']) & (pdf['up_move'] > 0),
        pdf['up_move'],
        0
    )
    pdf['-dm'] = np.where(
        (pdf['down_move'] > pdf['up_move']) & (pdf['down_move'] > 0),
        pdf['down_move'],
        0
    )


    for period in periods:
        # Wilder's smoothing for ATR, +DM, -DM
        pdf[f'smoothed_+dm_{period}'] = pdf['+dm'].ewm(alpha=1 / period, adjust=False).mean()
        pdf[f'smoothed_-dm_{period}'] = pdf['-dm'].ewm(alpha=1 / period, adjust=False).mean()

        # Calculate +DI and -DI
        pdf[f'+di_{period}'] = (pdf[f'smoothed_+dm_{period}'] / pdf[f'atr{period}']) * 100
        pdf[f'-di_{period}'] = (pdf[f'smoothed_-dm_{period}'] / pdf[f'atr{period}']) * 100

        # Calculate DX
        pdf[f'dx_{period}'] = (abs(pdf[f'+di_{period}'] - pdf[f'-di_{period}']) /
                               (pdf[f'+di_{period}'] + pdf[f'-di_{period}'])) * 100

        # Calculate ADX using Wilder's smoothing
        pdf[f'adx{period}'] = pdf[f'dx_{period}'].ewm(alpha=1 / period, adjust=False).mean()

        # Drop intermediate columns if desired
        pdf.drop(columns=[
            f'smoothed_+dm_{period}',
            f'smoothed_-dm_{period}',
            f'+di_{period}',
            f'-di_{period}',
            f'dx_{period}'
        ], inplace=True)

    # Drop other intermediate columns
    pdf.drop(columns=['up_move', 'down_move', '+dm', '-dm'], inplace=True)


    return pdf


# UDF to calculate the MACD and related indicators while retaining original columns
def calculate_macd(pdf):
    # Calculate the 12-period and 26-period EMAs
    pdf['ema_short'] = pdf['close'].ewm(span=12, adjust=False).mean()
    pdf['ema_long'] = pdf['close'].ewm(span=26, adjust=False).mean()

    # Calculate MACD line
    pdf['macd'] = pdf['ema_short'] - pdf['ema_long']

    # Calculate Signal line (9-period EMA of MACD)
    pdf['macdsignal'] = pdf['macd'].ewm(span=9, adjust=False).mean()

    # Calculate MACD histogram
    pdf['macdhist'] = pdf['macd'] - pdf['macdsignal']

    pdf = pdf.drop(['ema_short', 'ema_long'], axis=1)

    # Return the original DataFrame with the added columns for MACD, macdsignal, and macdhist
    return pdf

def macd(df):
    # Get the schema of the input DataFrame
    new_schema = StructType(df.schema.fields + [
        StructField("macd", DoubleType(), True),
        StructField("macdsignal", DoubleType(), True),
        StructField("macdhist", DoubleType(), True)
    ])

    # Apply the function to compute MACD, signal line, and histogram for each ticker group using applyInPandas
    df = df.groupby("ticker").applyInPandas(calculate_macd, schema=new_schema)

    return df
def calculate_rsi_group(pdf, period):
    pdf['ema_gains'] = calculate_atr_udf(pdf['gain'], period)
    pdf['ema_losses'] = calculate_atr_udf(pdf['loss'], period)

        # Calculate RS (Relative Strength)
    pdf['rs'] = pdf['ema_gains'] / pdf['ema_losses']

        # Calculate RSI
    pdf[f'rsi{period}'] = 100 - (100 / (1 + pdf['rs']))

        # Drop intermediate columns 'ema_gains', 'ema_losses', and 'rs'
    pdf = pdf.drop(columns=['ema_gains', 'ema_losses', 'rs'])

    return pdf

def rsi(df):
    periods = (14, 20)  # Define RSI periods

    # Window specification to get previous close
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate delta (difference between the current close and the previous close)
    df = df.withColumn('delta', F.expr('close - lag(close, 1) OVER (PARTITION BY ticker ORDER BY timestamp)'))

    # Calculate gain and loss
    df = df.withColumn('gain', F.when(F.col('delta') > 0, F.col('delta')).otherwise(0))
    df = df.withColumn('loss', F.when(F.col('delta') < 0, F.abs(F.col('delta'))).otherwise(0))

    # Function to calculate EMA for gains and losses and return RSI, dropping intermediate columns

    # Loop through each period (e.g., 14 and 20)
    for period in periods:
        # Define the schema for each period (keeping all original fields and adding RSI columns)
        schema = df.schema

        new_schema = StructType(schema.fields + [
            StructField(f'rsi{period}', DoubleType(), True)
        ])

        # Apply the UDF by grouping the data by 'ticker' and applying the Pandas UDF on each group
        df = df.groupBy('ticker').applyInPandas(
            lambda pdf: calculate_rsi_group(pdf, period),
            schema=new_schema
        )

    # Drop intermediate columns (delta, gain, loss)
    df = df.drop('delta', 'gain', 'loss')

    return df

# Function to calculate the stochastic oscillator (%K and %D) for both 14-period and 20-period
def stoch(df):
    # Loop through each period (e.g., 14 and 20)
    periods = (14, 20)
    smooth_k = 3
    for period in periods:
        # Window specification for calculating the high-low range over the given period
        window_spec = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-period + 1, 0)

        # Calculate the highest high and the lowest low over the period
        df = df.withColumn(f'highest_high_{period}', F.max('high').over(window_spec))
        df = df.withColumn(f'lowest_low_{period}', F.min('low').over(window_spec))

        # Calculate %K for the current period
        df = df.withColumn(f'%K_{period}', F.expr(f'((close - lowest_low_{period}) / (highest_high_{period} - lowest_low_{period})) * 100'))

        # Window specification for calculating %D (SMA of %K over the smoothing period)
        window_spec_smooth = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-smooth_k + 1, 0)

        # Calculate %D as the 3-period simple moving average of %K
        df = df.withColumn(f'stoch{period}', F.avg(f'%K_{period}').over(window_spec_smooth))

        # Drop intermediate columns for the current period
        df = df.drop(f'highest_high_{period}', f'lowest_low_{period}', f'%K_{period}')

    return df


# Function to calculate On-Balance Volume (OBV)
def obv(df):
    """
    Calculate On-Balance Volume (OBV) for time series data.

    Args:
        df: DataFrame with columns: ticker, timestamp, close, volume

    Returns:
        DataFrame with additional OBV column
    """
    # Window specification to get the previous close price
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate price change direction
    df = df.withColumn('close_diff',
                       F.when(F.col('close') > F.lag('close').over(window_spec), F.lit(1))
                       .when(F.col('close') < F.lag('close').over(window_spec), F.lit(-1))
                       .otherwise(F.lit(0)))

    # Calculate OBV changes
    df = df.withColumn('obv_change',
                       F.col('close_diff') * F.col('volume'))

    # Calculate cumulative OBV
    df = df.withColumn('obv',
                       F.sum('obv_change').over(window_spec.rowsBetween(Window.unboundedPreceding, 0)))

    # Drop intermediate columns
    df = df.drop('close_diff', 'obv_change')

    return df



def inside_bar(df):
    # Define window specification for calculating inside bar

    window_spec = Window.partitionBy('ticker').orderBy('timestamp')
    df = df.withColumn('inside_bar', (F.col('low') > F.lag('low', 1).over(window_spec)) &
                       (F.col('high') < F.lag('high', 1).over(window_spec)))

    return df

def signal_bar(df):
    # Define window specification for calculating signal bar

    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    df = df.withColumn('signal_bar_bull', (F.col('open') < F.lag('low', 1).over(window_spec)) &
                       (F.col('close') > F.col('open')) &
                       ((F.col('high') - F.col('close')) < 0.2 * (F.col('high') - F.col('low'))))

    df = df.withColumn('signal_bar_bear', (F.col('open') > F.lag('high', 1).over(window_spec)) &
                       (F.col('close') < F.col('open')) &
                       ((F.col('close') - F.col('low')) < 0.2 * (F.col('high') - F.col('low'))))

    return df
def mBR(df):
    # Define window specifications for calculating mBR (Multiple Bar Range)

    # Calculate br2: The range over the last 2 periods
    df = df.withColumn('br2',
        F.greatest(F.col('high'), F.lag('high', 1).over(Window.partitionBy('ticker').orderBy('timestamp'))) -
        F.least(F.col('low'), F.lag('low', 1).over(Window.partitionBy('ticker').orderBy('timestamp')))
    )

    # Calculate br3: The range over the last 3 periods
    df = df.withColumn('br3',
        F.greatest(F.col('high'),
                   F.lag('high', 1).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 2).over(Window.partitionBy('ticker').orderBy('timestamp'))) -
        F.least(F.col('low'),
                F.lag('low', 1).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 2).over(Window.partitionBy('ticker').orderBy('timestamp')))
    )

    # Calculate br4: The range over the last 4 periods
    df = df.withColumn('br4',
        F.greatest(F.col('high'),
                   F.lag('high', 1).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 2).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 3).over(Window.partitionBy('ticker').orderBy('timestamp'))) -
        F.least(F.col('low'),
                F.lag('low', 1).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 2).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 3).over(Window.partitionBy('ticker').orderBy('timestamp')))
    )

    # Calculate br8: The range over the last 8 periods
    df = df.withColumn('br8',
        F.greatest(F.col('high'),
                   F.lag('high', 1).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 2).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 3).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 4).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 5).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 6).over(Window.partitionBy('ticker').orderBy('timestamp')),
                   F.lag('high', 7).over(Window.partitionBy('ticker').orderBy('timestamp'))) -
        F.least(F.col('low'),
                F.lag('low', 1).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 2).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 3).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 4).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 5).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 6).over(Window.partitionBy('ticker').orderBy('timestamp')),
                F.lag('low', 7).over(Window.partitionBy('ticker').orderBy('timestamp')))
    )

    return df

def midRange(df):
    # Use PySpark functions to calculate mid_range efficiently
    df = df.withColumn('mid_range', F.expr('low + (high - low) / 2'))
    return df

def doji(df):
    # Calculate doji indicator using PySpark functions
    df = df.withColumn('doji', F.abs(F.col('close') - F.col('open')) <= 0.001 * F.col('close'))
    return df

def KC_band(df):
    # Use existing column 'ema20' to set 'kcmiddle'
    df = df.withColumn('kcmiddle', F.col('ema20'))
    return df

def reversals(df):
    # Define window specification for calculating reversals
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate bullish and bearish reversals using PySpark functions
    df = df.withColumn('reversal_bull', F.when((F.col('low') < F.lag('low', 1).over(window_spec)) &
                                               (F.col('close') > F.col('open')) &
                                               ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                                (F.col('volume') > F.col('vma30'))), True).otherwise(False))
    df = df.withColumn('reversal_bear', F.when((F.col('high') > F.lag('high', 1).over(window_spec)) &
                                               (F.col('close') < F.col('open')) &
                                               ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                                (F.col('volume') > F.col('vma30'))), True).otherwise(False))

    return df


def key_reversals(df):
    # Define window specification for calculating key reversals
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate key bullish and bearish reversals using PySpark functions
    df = df.withColumn('key_reversal_bull', F.when((F.col('low') < F.lag('low', 1).over(window_spec)) &
                                                   (F.col('close') > F.lag('close', 1).over(window_spec)) &
                                                   (F.col('close') > F.col('open')) &
                                                   ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                                    (F.col('volume') > F.col('vma30'))), True).otherwise(False))
    df = df.withColumn('key_reversal_bear', F.when((F.col('high') > F.lag('high', 1).over(window_spec)) &
                                                   (F.col('close') < F.lag('close', 1).over(window_spec)) &
                                                   (F.col('close') < F.col('open')) &
                                                   ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                                    (F.col('volume') > F.col('vma30'))), True).otherwise(False))

    return df


def cpr(df):
    # Define window specification for calculating CPR (Central Pivot Range)
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate bullish and bearish CPR using PySpark functions
    df = df.withColumn('cpr_bull', F.when((F.col('close') > F.lag('high', 1).over(window_spec)) &
                                          ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                           (F.col('volume') > F.col('vma30'))), True).otherwise(False))
    df = df.withColumn('cpr_bear', F.when((F.col('close') < F.lag('low', 1).over(window_spec)) &
                                          ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                           (F.col('volume') > F.col('vma30'))), True).otherwise(False))

    return df


def cgc(df):
    # Define window specification for calculating CGC (Close Gap Continuation)
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate bullish and bearish CGC using PySpark functions
    df = df.withColumn('cgc_bull', F.when((F.col('close') > F.lag('high', 1).over(window_spec)) &
                                          (F.col('open') > F.lag('close', 1).over(window_spec)) &
                                          (F.col('low') > F.lag('close', 1).over(window_spec)) &
                                          ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                           (F.col('volume') > F.col('vma30'))), True).otherwise(False))
    df = df.withColumn('cgc_bear', F.when((F.col('close') < F.lag('low', 1).over(window_spec)) &
                                          (F.col('open') < F.lag('close', 1).over(window_spec)) &
                                          (F.col('high') < F.lag('close', 1).over(window_spec)) &
                                          ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                           (F.col('volume') > F.col('vma30'))), True).otherwise(False))

    return df


def outside_bar(df):
    # Define window specification for calculating outside bar
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate outside bar (up and down) using PySpark functions
    df = df.withColumn('outside_bar_up', F.when((F.col('close') > F.col('open')) &
                                                (F.col('low') < F.lag('low', 1).over(window_spec)) &
                                                (F.col('high') > F.lag('high', 1).over(window_spec)) &
                                                ((F.col('high') - F.col('close')) < 0.33 * (F.col('high') - F.col('low'))) &
                                                ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                                 (F.col('volume') > F.col('vma30'))), True).otherwise(False))
    df = df.withColumn('outside_bar_down', F.when((F.col('close') < F.col('open')) &
                                                  (F.col('low') < F.lag('low', 1).over(window_spec)) &
                                                  (F.col('high') > F.lag('high', 1).over(window_spec)) &
                                                  ((F.col('close') - F.col('low')) < 0.33 * (F.col('high') - F.col('low'))) &
                                                  ((F.col('volume') > F.lag('volume', 1).over(window_spec)) |
                                                   (F.col('volume') > F.col('vma30'))), True).otherwise(False))

    return df
def noRange(df):
    # Define window specification for calculating no range bars
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate nr7, nr6, nr5, nr4, nr3, and nr2 using PySpark functions
    for i in range(2, 8):
        condition = F.col('tr') < F.lag('tr', 1).over(window_spec)
        for j in range(2, i):
            condition = condition & (F.col('tr') < F.lag('tr', j).over(window_spec))
        df = df.withColumn(f'nr{i}', condition)

    return df

def Hooks(df):
    # Define window specification for calculating hooks
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate bearhook and bullhook using PySpark functions
    df = df.withColumn('bearhook', F.when((F.col('open') < F.lag('low', 1).over(window_spec)) &
                                          (F.col('close') > F.lag('close', 1).over(window_spec)) &
                                          (F.col('tr') < F.lag('tr', 1).over(window_spec)), True).otherwise(False))
    df = df.withColumn('bullhook', F.when((F.col('open') > F.lag('high', 1).over(window_spec)) &
                                          (F.col('close') < F.lag('close', 1).over(window_spec)) &
                                          (F.col('tr') < F.lag('tr', 1).over(window_spec)), True).otherwise(False))

    return df


def wideSpread(df):
    # Define window specification for calculating wide spread bars
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Use a vectorized approach for calculating ws2 to ws7
    for i in range(2, 8):
        condition = F.col('tr') > F.lag('tr', 1).over(window_spec)
        for j in range(2, i):
            condition = condition & (F.col('tr') > F.lag('tr', j).over(window_spec))
        df = df.withColumn(f'ws{i}', condition)

    return df



def tr3(df):
    # Define window specification for calculating tr3
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate tr3 using PySpark functions
    df = df.withColumn('tr3',
        (F.col('tr') < 0.5 * F.col('atr14')) &
        (F.lag('tr', 1).over(window_spec) < 0.5 * F.lag('atr14', 1).over(window_spec)) &
        (F.lag('tr', 2).over(window_spec) < 0.5 * F.lag('atr14', 2).over(window_spec))
    )

    return df

def stalling_bear(df):
    # Define window specification for calculating stalling bear
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate stalling bear using PySpark functions
    df = df.withColumn('stalling_bear', F.when((F.col('volume') > F.lag('volume', 1).over(window_spec)) &
                                               (F.col('close') > F.lag('close', 1).over(window_spec)) &
                                               ((F.col('high') - F.col('close')) > 0.5 * (F.col('high') - F.col('low'))) &
                                               ((F.col('high') - F.col('open')) < 0.2 * (F.col('high') - F.col('low'))), True).otherwise(False))

    return df
def LRHCHV(df):
    # Define window specification for calculating LRHCHV
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Precompute lagged volume to optimize performance
    df = df.withColumn('lag_volume', F.lag('volume', 1).over(window_spec))

    # Calculate lrhchv and lrlchv using PySpark functions
    df = df.withColumn('lrhchv', F.when((F.col('tr') > 1.75 * F.col('atr14')) &
                                        ((F.col('high') - F.col('close')) < 0.2 * (F.col('high') - F.col('low'))) &
                                        (F.col('close') > F.col('open')) &
                                        ((F.col('volume') > F.col('lag_volume')) & (F.col('volume') > F.col('vma30'))),
                                        True).otherwise(False))

    df = df.withColumn('lrlchv', F.when((F.col('tr') > 1.75 * F.col('atr14')) &
                                        ((F.col('close') - F.col('low')) < 0.2 * (F.col('high') - F.col('low'))) &
                                        (F.col('close') < F.col('open')) &
                                        ((F.col('volume') > F.col('lag_volume')) & (F.col('volume') > F.col('vma30'))),
                                        True).otherwise(False))

    # Drop intermediate column if not needed later
    df = df.drop('lag_volume')

    return df

def LVLR(df):
    # Calculate lv_day and lr_day using PySpark functions
    df = df.withColumn('lv_day', F.col('volume') < 0.5 * F.col('vma30'))
    df = df.withColumn('lr_day', F.col('tr') < 0.6 * F.col('atr14'))

    return df

def VLV(df):
    # Calculate vlv_day using PySpark functions
    df = df.withColumn('vlv_day', F.col('volume') < 0.4 * F.col('vma30'))

    return df

def exhaust(df):
    # Calculate exhaust_bar_up and exhaust_bar_down using PySpark functions
    df = df.withColumn('exhaust_bar_up', F.when((F.col('tr') > 2 * F.col('atr14')) &
                                               (F.col('volume') > 1.5 * F.col('atr14')) &
                                               (F.col('close') > F.col('open')), True).otherwise(False))
    df = df.withColumn('exhaust_bar_down', F.when((F.col('tr') > 2 * F.col('atr14')) &
                                                 (F.col('volume') > 1.5 * F.col('atr14')) &
                                                 (F.col('close') < F.col('open')), True).otherwise(False))

    return df

def tbblbg(df):
    # Define window specification for calculating tbblbg
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Pre-compute lagged columns to avoid repetitive calculations
    df = df.withColumn('lag_volume', F.lag('volume', 1).over(window_spec))
    df = df.withColumn('lag_close', F.lag('close', 1).over(window_spec))
    df = df.withColumn('lag_high', F.lag('high', 1).over(window_spec))
    df = df.withColumn('lag_low', F.lag('low', 1).over(window_spec))

    # Calculate tb_up and tb_down using PySpark functions
    tb_condition = (F.col('high') - F.col('low')) > 2 * F.col('atr20')
    df = df.withColumn('tb_up', F.when(tb_condition &
                                       (F.col('volume') > F.col('lag_volume')) &
                                       ((F.col('high') - F.col('close')) < 0.33 * (F.col('high') - F.col('low'))), True).otherwise(False))
    df = df.withColumn('tb_down', F.when(tb_condition &
                                         (F.col('volume') > F.col('lag_volume')) &
                                         ((F.col('close') - F.col('low')) < 0.33 * (F.col('high') - F.col('low'))), True).otherwise(False))

    # Calculate bl_up and bl_down using PySpark functions
    bl_up_condition = (F.col('low') > F.col('lag_close')) & (F.col('low') < F.col('lag_high'))
    bl_down_condition = (F.col('high') < F.col('lag_close')) & (F.col('high') > F.col('lag_low'))
    volume_condition = (F.col('volume') > F.col('lag_volume')) | (F.col('volume') > F.col('vma30'))

    df = df.withColumn('bl_up', F.when(bl_up_condition & volume_condition, True).otherwise(False))
    df = df.withColumn('bl_down', F.when(bl_down_condition & volume_condition, True).otherwise(False))

    # Calculate bg_up and bg_down using PySpark functions
    df = df.withColumn('bg_up', F.when((F.col('low') > F.col('lag_high')) & volume_condition, True).otherwise(False))
    df = df.withColumn('bg_down', F.when((F.col('high') < F.col('lag_low')) & volume_condition, True).otherwise(False))

    # Drop intermediate lagged columns to keep the DataFrame clean
    df = df.drop('lag_volume', 'lag_close', 'lag_high', 'lag_low')

    return df

def rBRN(df):
    # Define window specification for calculating rolling minimum
    window_spec_20 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-19, 0)
    window_spec_30 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-29, 0)
    window_spec_40 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-39, 0)

    # Calculate rolling minimum for br2, br3, br4, and br8 using PySpark functions
    df = df.withColumn('rbr2', F.min('br2').over(window_spec_20))
    df = df.withColumn('rbr3', F.min('br3').over(window_spec_20))
    df = df.withColumn('rbr4', F.min('br4').over(window_spec_30))
    df = df.withColumn('rbr8', F.min('br8').over(window_spec_40))

    return df


### Stage 4 level data

def KC_band_extra(df):
    # Calculate kcupper and kclower using PySpark functions
    df = df.withColumn('kcupper', F.col('kcmiddle') + 2 * F.col('atr10'))
    df = df.withColumn('kclower', F.col('kcmiddle') - 2 * F.col('atr10'))

    return df

def tbblbg_cond(df):
    # Calculate tbblbg_up and tbblbg_down using PySpark functions
    df = df.withColumn('tbblbg_up', F.col('tb_up') | F.col('bl_up') | F.col('bg_up'))
    df = df.withColumn('tbblbg_down', F.col('tb_down') | F.col('bl_down') | F.col('bg_down'))

    return df

def exhaust_cond(df):
    # Define window specification for calculating exhaust conditions
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate exhaust_condition_bull and exhaust_condition_bear using PySpark functions
    df = df.withColumn('exhaust_condition_bull', F.when((F.lag('exhaust_bar_up', 1).over(window_spec) == True) &
                                                        (F.col('close') > F.lag('close', 1).over(window_spec)), True).otherwise(False))
    df = df.withColumn('exhaust_condition_bear', F.when((F.lag('exhaust_bar_down', 1).over(window_spec) == True) &
                                                        (F.col('close') < F.lag('close', 1).over(window_spec)), True).otherwise(False))

    return df

def mBNR_cond(df):
    # Calculate twobnr, threebnr, fourbnr, and eightbnr using PySpark functions
    df = df.withColumn('twobnr', F.col('br2') <= F.col('rbr2'))
    df = df.withColumn('threebnr', F.col('br3') <= F.col('rbr3'))
    df = df.withColumn('fourbnr', F.col('br4') <= F.col('rbr4'))
    df = df.withColumn('eightbnr', F.col('br8') <= F.col('rbr8'))

    return df


### Stage 5 level data

def tbblbg_num(df):
    # Convert tbblbg_up and tbblbg_down boolean values to numerical representations
    df = df.withColumn('tbblbg_up_num', F.when(F.col('tbblbg_up') == True, 1).otherwise(0))
    df = df.withColumn('tbblbg_down_num', F.when(F.col('tbblbg_down') == True, 1).otherwise(0))

    return df
def exhaust_final(df):
    # Define window specification for calculating exhaust final conditions
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate exhaust_trade_bull and exhaust_trade_bear using PySpark functions
    df = df.withColumn('exhaust_trade_bull', F.when((F.lag('exhaust_condition_bull', 1).over(window_spec) == True) &
                                                    (F.col('close') > F.lag('high', 1).over(window_spec)), True).otherwise(False))
    df = df.withColumn('exhaust_trade_bear', F.when((F.lag('exhaust_condition_bear', 1).over(window_spec) == True) &
                                                    (F.col('close') < F.lag('low', 1).over(window_spec)), True).otherwise(False))

    return df

def runaway(df):
    # Define window specifications for calculating runaway conditions
    window_spec_21 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-20, 0)
    window_spec_30 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-29, 0)
    window_spec_5 = Window.partitionBy('ticker').orderBy('timestamp').rowsBetween(-4, 0)

    # Calculate runaway conditions using PySpark functions
    df = df.withColumn('runaway_up_521', F.sum('tbblbg_up_num').over(window_spec_21))
    df = df.withColumn('runaway_down_521', F.sum('tbblbg_down_num').over(window_spec_21))
    df = df.withColumn('runaway_up_1030', F.sum('tbblbg_up_num').over(window_spec_30))
    df = df.withColumn('runaway_down_1030', F.sum('tbblbg_down_num').over(window_spec_30))
    df = df.withColumn('runaway_up_0205', F.sum('tbblbg_up_num').over(window_spec_5))
    df = df.withColumn('runaway_down_0205', F.sum('tbblbg_down_num').over(window_spec_5))

    return df
# First extra indicators - trend day, future performances

# extra1, Level1

def td_metric1(df):
    # Calculate tdm1 using PySpark functions
    tdm1a = ((F.col('close') - F.col('low')) / (F.col('high') - F.col('low'))) >= 0.85
    tdm1b = ((F.col('high') - F.col('close')) / (F.col('high') - F.col('low'))) >= 0.85

    df = df.withColumn('tdm1', tdm1a | tdm1b)

    return df


def td_metric2(df):
    # Calculate tdm2 using PySpark functions
    df = df.withColumn('tdm2',
                       ((F.col('close') - F.col('low')) >= 0.75 * F.col('atr14')) |
                       ((F.col('high') - F.col('close')) >= 0.75 * F.col('atr14')))
    return df

def td_metric3(df):
    # Calculate tdm3 using PySpark functions
    df = df.withColumn('tdm3',
                       (((F.col('close') - F.col('open')) / F.col('open')) * 100 >= 1.5) |
                       (((F.col('open') - F.col('close')) / F.col('open')) * 100 >= 1.5))
    return df

def return_metrics(df):
    # Define window specifications without .rowsBetween()
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate return metrics using PySpark functions and correct window specification
    df = df.withColumn('return_1day', (F.lead('close', 1).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_2days', (F.lead('close', 2).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_3days', (F.lead('close', 3).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_4days', (F.lead('close', 4).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_1week', (F.lead('close', 5).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_2weeks', (F.lead('close', 10).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_3weeks', (F.lead('close', 15).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_1month', (F.lead('close', 21).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_2months', (F.lead('close', 42).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_1quarter', (F.lead('close', 63).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_2quarters', (F.lead('close', 126).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_3quarters', (F.lead('close', 189).over(window_spec) - F.col('close')) / F.col('close'))
    df = df.withColumn('return_1year', (F.lead('close', 252).over(window_spec) - F.col('close')) / F.col('close'))

    return df# extra1, Level2

def trend_metrics(df):
    # Calculate trend day and likely trend day using PySpark functions
    df = df.withColumn('trend_day', F.col('tdm1') & F.col('tdm2') & F.col('tdm3'))
    df = df.withColumn('likely_trend_day', F.col('tdm1') | F.col('tdm2') | F.col('tdm3'))

    # Define window specification for future trend calculations
    window_spec = Window.partitionBy('ticker').orderBy('timestamp')

    # Calculate future trend day metrics using PySpark functions
    df = df.withColumn('ftd', F.lead('trend_day', 1).over(window_spec))
    df = df.withColumn('fltd', F.lead('likely_trend_day', 1).over(window_spec))
    df = df.withColumn('ftdm1', F.lead('tdm1', 1).over(window_spec))
    df = df.withColumn('ftdm2', F.lead('tdm2', 1).over(window_spec))
    df = df.withColumn('ftdm3', F.lead('tdm3', 1).over(window_spec))

    return df
def calculate_indicators(tmp_df):
    ###Level 2 data

    tmp_df = calculate_moving_average(tmp_df)

    tmp_df = calculate_ATR(tmp_df)

    tmp_df = volume_MA(tmp_df)

    tmp_df = BB_band(tmp_df)

    tmp_df = yearly_high_low(tmp_df)

    tmp_df = adx(tmp_df)

    tmp_df = macd(tmp_df)

    tmp_df = rsi(tmp_df)

    tmp_df = stoch(tmp_df)

    tmp_df = obv(tmp_df)

    tmp_df = inside_bar(tmp_df)

    tmp_df = signal_bar(tmp_df)

    tmp_df = mBR(tmp_df)

    tmp_df = doji(tmp_df)

    tmp_df = midRange(tmp_df)

    # next set
    
    tmp_df = KC_band(tmp_df)

    tmp_df = reversals(tmp_df)

    tmp_df = key_reversals(tmp_df)

    tmp_df = cpr(tmp_df)

    tmp_df = cgc(tmp_df)

    tmp_df = outside_bar(tmp_df)

    tmp_df = noRange(tmp_df)

    tmp_df = Hooks(tmp_df)

    tmp_df = wideSpread(tmp_df)

    tmp_df = tr3(tmp_df)

    tmp_df = stalling_bear(tmp_df)

    tmp_df = LRHCHV(tmp_df)

    tmp_df = LVLR(tmp_df)

    tmp_df = VLV(tmp_df)

    tmp_df = exhaust(tmp_df)

    tmp_df = tbblbg(tmp_df)

    tmp_df = rBRN(tmp_df)

    ###next set

    tmp_df = KC_band_extra(tmp_df)

    tmp_df = tbblbg_cond(tmp_df)

    tmp_df = exhaust_cond(tmp_df)

    tmp_df = mBNR_cond(tmp_df)

    ###next_Set

    tmp_df = tbblbg_num(tmp_df)

    tmp_df = exhaust_final(tmp_df)

    tmp_df = runaway(tmp_df)

   ### next_set

    tmp_df = td_metric1(tmp_df)

    tmp_df = td_metric2(tmp_df)

    tmp_df = td_metric3(tmp_df)

    tmp_df = return_metrics(tmp_df)

    tmp_df = trend_metrics(tmp_df)

    return tmp_df


def insert_updated_rows(jdbc_url, jdbc_properties, data_sdf):
    try:
        # Write the Spark DataFrame back to the PostgreSQL database using JDBC
        data_sdf.write \
            .jdbc(url=jdbc_url, table="key_indicators_alltickers", mode="append", properties=jdbc_properties)

        print("Data inserted successfully into the database using JDBC")

    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

def check_spark_session():
    try:
        # Try a simple action to ensure the session is active
        spark.sparkContext.range(1).count()
        print("SparkSession is active.")
    except Exception as e:
        print(f"SparkSession is inactive or encountered an error: {e}")
        sys.exit(1)



if __name__ == '__main__':

    # record start time
    start = time.time()

    # Database connection parameters
    jdbc_url = "jdbc:postgresql://localhost:5432/markets_technicals"
    jdbc_properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    # Set the Python path for the PySpark workers and driver
    python_path = "C:/ProgramData/Anaconda3/python.exe"
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    # Initialize SparkSession with configuration to use 16 cores and 120GB memory (leaving some for the OS)
    spark = SparkSession.builder \
        .appName("Key Indicators Processing") \
        .config("spark.master", "local[*]") \
        .config("spark.executor.memory", "110g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.default.parallelism", "32") \
        .config("spark.local.dir", "C:/Users/uvdsa/SparkTemp") \
        .config("spark.jars", "C:\jdbc_drivers\postgresql-42.7.4.jar") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")\
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")\
        .config("spark.pyspark.python", python_path) \
        .config("spark.hadoop.io.nativeio.enabled", "false") \
        .config("spark.pyspark.driver.python", python_path) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:

        # delete existing rows
        print("deleting the existing rows")
        delete_existing_rows()

        # get the modified rows
        print("getting all price data since april 2021 for full calculations")
        allprice_df = get_allprice_data_sinceapril2021(jdbc_url, jdbc_properties)



        # Add date column from timestamp
        allprice_df = allprice_df.withColumn('date', F.date_format('timestamp', 'yyyy-MM-dd'))

        # Get key indicators for all data (no looping through individual tickers)
        print("Calculating indicators")
        allprice_df = allprice_df.withColumn("ticker", F.col("ticker").cast("string"))
        allprice_df = allprice_df.sort("ticker", "date")
        allprice_df = calculate_indicators(allprice_df)


        allprice_df = allprice_df.fillna(0)

        # Filtering the data after 2023-03-31 using the 'date' column instead of 'timestamp'
        new_df = allprice_df.filter(F.col('date') > F.lit('2023-03-31'))

        print("Calculating indicators-done")

        #print("converting to pandas")
        #pandas_df = new_df.toPandas()

        # Write DataFrame to local CSV file
        #path_str = r"D:\data\db\key_indicators_population_delta.csv"
        #print("writing to CSV")
        #pandas_df.to_csv(path_str, index=False)
        #new_df.write.csv(path_str, header=True, mode="overwrite")

        num_columns = len(new_df.columns)

        # Print the number of columns
        print(f"Number of columns: {num_columns}")

        # Insert the modified rows
        print("Inserting modified rows")
        # Call this before insert_updated_rows to check the session's status
        insert_updated_rows(jdbc_url, jdbc_properties, new_df)

        # Commit transaction if everything is successful
        print("Data has been successfully committed to the database.")

    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

    finally:
        # Stop Spark session
        spark.stop()
        # Record end time
        end = time.time()
        # Print the difference between start and end time in minutes
        print("The time of execution of above program is:", ((end - start) * 10 ** 3) / 60000, "minutes")

sys.exit(0)



    























