# This script is upload net new eod data for all stocks, specific stocks, top 10 stocks per industry group to DB
# Order of scripts: IQDelta, Plurality_RS1-Daily, KeyindicatorsPopulation_Delta, Plurality-RS-Upload, update_excel_RS, plurality1_plots

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import datetime
import sys
import socket
import iqfeedTest as iq
import time
import os

# Set the Python path for the PySpark workers and driver
python_path = "C:/ProgramData/Anaconda3/python.exe"
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Initialize SparkSession with configuration to use 16 cores and 120GB memory (leaving some for the OS)
spark = SparkSession.builder \
    .appName("EOD Data Processing") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "110g")  \
    .config("spark.driver.memory", "16g")  \
    .config("spark.sql.shuffle.partitions", "32") \
    .config("spark.default.parallelism", "32")  \
    .config("spark.jars", "/path_to/postgresql-42.2.24.jar") \
    .config("spark.pyspark.python", python_path) \
    .config("spark.pyspark.driver.python", python_path) \
    .getOrCreate()


def get_all_tickers_defaultDates(spark, jdbc_url, jdbc_properties):
    # Load all tickers from PostgreSQL using JDBC
    df = spark.read.jdbc(url=jdbc_url, table="all_tickers", properties=jdbc_properties)

    # Set default date to 1950-01-01 and format it
    df = df.withColumn("next_timestamp_date", F.lit("19500101"))

    return df.select("ticker", "next_timestamp_date")

def get_dates_all_tickers(spark, jdbc_url, jdbc_properties):
    # Load dates and tickers from PostgreSQL using JDBC
    df = spark.read.jdbc(url=jdbc_url, table="max_date_price_view", properties=jdbc_properties)

    # Increment timestamp by 1 day and format date
    df = df.withColumn("next_timestamp", F.date_add(df["last_timestamp"], 1))
    df = df.withColumn("next_timestamp_date", F.date_format(df["next_timestamp"], "yyyyMMdd"))

    # Select necessary columns
    return df.select("ticker", "next_timestamp_date")

def get_all_tickers_defaultDates(conn):

    cursor = conn.cursor()
    postgreSQL_select_Query = "select ticker from all_tickers"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['ticker'])

    df['def_date'] = pd.Timestamp('1950-01-01')
    df['date'] = df['def_date'].dt.strftime('%Y%m%d')

    df.drop(['def_date'], axis=1, inplace=True)


    return df

def update_date_tickers_all(def_df,df):
    #merge data frames,overriding overlaps

    merged_df = pd.merge(def_df,df,on='ticker', how='outer')

    merged_df['next_timestamp_date'] = merged_df['next_timestamp_date'].fillna(merged_df['date'])

    merged_df.drop(['date'], axis=1, inplace=True)

    merged_df = merged_df.drop(index=0)

    merged_dates = merged_df.set_index('ticker')['next_timestamp_date'].to_dict()

    return merged_dates


def get_historical_data(ticker, date):
    # Simulating historical data retrieval
    message = f"HDT,{ticker},{date},20250101\n"
    message = bytes(message, encoding='utf-8')

    # Open a socket to IQFeed server and get the data
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 9100))
    sock.sendall(message)
    data = read_historical_data_socket(sock)
    sock.close()

    # If no data, return empty DataFrame
    if "!NO_DATA!" in data:
        return pd.DataFrame()

    # Process and convert data to DataFrame
    columns = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
    data = data.replace(",\n", "\n").split("\n")[:-1]
    rows = [row.split(",") for row in data]
    return pd.DataFrame(rows, columns=columns)

def read_historical_data_socket(sock, recv_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    while True:
        data = str(sock.recv(recv_buffer), encoding='utf-8')
        buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break
    # Remove the end message string
    buffer = buffer[:-12]
    return buffer

def save_to_postgresql(spark_df, table_name, jdbc_url, jdbc_properties):
    # Write Spark DataFrame back to PostgreSQL
    spark_df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=jdbc_properties)

def fetch_and_save_data(ticker_record):
    ticker = ticker_record["ticker"]
    date = ticker_record["next_timestamp_date"]
    print(f"getting data for {ticker}")
    df = get_historical_data(ticker, date)
    if not df.empty:
        return df
    return None

if __name__ == '__main__':

    # record start time
    start = time.time()

    # JDBC connection properties
    jdbc_url = "jdbc:postgresql://localhost:5432/Plurality"
    jdbc_properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    # Fetch all tickers with default dates
    default_dates_df = get_all_tickers_defaultDates(spark, jdbc_url, jdbc_properties)

    # Fetch tickers with last timestamp from the view
    date_tickers_df = get_dates_all_tickers(spark, jdbc_url, jdbc_properties)

    # Update tickers and their dates
    final_tickers_df = update_date_tickers_all(default_dates_df, date_tickers_df)

    # Convert to Pandas for easy processing in historical data fetch
    pandas_df = final_tickers_df.toPandas()

    # Split tickers for parallel processing
    ticker_list = pandas_df.to_dict(orient="records")

    # Parallelize the data fetching using Spark
    rdd = spark.sparkContext.parallelize(ticker_list)

    iq.launch_service()

    # Process in parallel
    results_rdd = rdd.map(fetch_and_save_data).filter(lambda x: x is not None)

    # Convert to Spark DataFrame and save results to PostgreSQL
    final_result_df = results_rdd.toDF()
    save_to_postgresql(final_result_df, "usstockseod", jdbc_url, jdbc_properties)

    spark.stop()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3)/60000, "minutes")

    sys.exit(0)
