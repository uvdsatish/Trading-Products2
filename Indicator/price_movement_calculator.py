import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
import sys
import time

def get_data_from_postgres(query, connection_params):
    """
    Connect to PostgreSQL and fetch data using the provided query.
    """
    conn = psycopg2.connect(**connection_params)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df

def copy_to_postgres(dataframe, table_name, connection_params):
    """
    Use COPY FROM with StringIO to copy a Pandas DataFrame into a PostgreSQL table.
    """
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    try:
        buffer = StringIO()
        dataframe.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def truncate_table(table_name, connection_params):
    """
    Truncate the table to remove all data before loading new data, but only if the table exists.
    """
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    try:
        check_table_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name = '{table_name}'
        );
        """
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            truncate_query = f"TRUNCATE {table_name}"
            cursor.execute(truncate_query)
            conn.commit()
    finally:
        cursor.close()
        conn.close()

def identify_pivot_points(ohlc_data: pd.DataFrame, window: int = 6):
    """
    Identify swing highs and lows (6-pivot highs and lows) independently of each other.
    """
    ohlc_data = ohlc_data.sort_values(by='date').reset_index(drop=True)

    # Initialize new columns for storing the pivot points
    ohlc_data['6pivothigh'] = np.nan
    ohlc_data['6pivotlow'] = np.nan

    # Detect pivot highs and lows independently
    for i in range(window, len(ohlc_data) - window):
        high_range = ohlc_data['high'].iloc[i - window:i + window + 1]
        low_range = ohlc_data['low'].iloc[i - window:i + window + 1]

        # Pivot high: the maximum in the surrounding window
        if ohlc_data['high'].iloc[i] == high_range.max():
            ohlc_data.at[i, '6pivothigh'] = ohlc_data['high'].iloc[i]

        # Pivot low: the minimum in the surrounding window
        if ohlc_data['low'].iloc[i] == low_range.min():
            ohlc_data.at[i, '6pivotlow'] = ohlc_data['low'].iloc[i]

    return ohlc_data

def calculate_drawdowns(ohlc_data: pd.DataFrame):
    """
    Calculate drawdowns, durations, and recovery times using independent 6-pivot highs and lows.
    Directly updates the ohlc_data DataFrame with row-level drawdown, duration, and recovery data.
    """
    ohlc_data['drawdown'] = np.nan
    ohlc_data['drawdown_duration'] = np.nan
    ohlc_data['drawdown_recovery_time'] = np.nan

    # Initialize variables
    max_high = ohlc_data['high'].iloc[0]
    max_high_index = 0
    drawdowns = []
    durations = []
    recoveries = []
    drawdown_in_progress = False
    trough_index = None
    lowest_trough = None

    for i in range(1, len(ohlc_data)):
        pivot_high = ohlc_data['6pivothigh'].iloc[i]
        pivot_low = ohlc_data['6pivotlow'].iloc[i]
        current_close = ohlc_data['close'].iloc[i]

        # Update max-high only if a higher value is found, and ensure max_high is non-zero
        if not np.isnan(pivot_high) and pivot_high > 0 and pivot_high > max_high:
            max_high = pivot_high
            max_high_index = i
            drawdown_in_progress = False
            trough_index = None
            lowest_trough = None

        # Only calculate drawdown if max_high is greater than zero
        if not np.isnan(pivot_low) and max_high > 0:
            drawdown = (max_high - pivot_low) / max_high * 100
            drawdowns.append(drawdown)

            duration = i - max_high_index
            durations.append(duration)

            ohlc_data.at[i, 'drawdown'] = drawdown
            ohlc_data.at[i, 'drawdown_duration'] = duration

            # Update the lowest trough and track its index
            if lowest_trough is None or pivot_low < lowest_trough:
                lowest_trough = pivot_low
                trough_index = i

            drawdown_in_progress = True

        # If a drawdown is in progress and the current close exceeds the max-high, calculate recovery
        if drawdown_in_progress and current_close > max_high:
            recovery_time = i - trough_index
            recoveries.append(recovery_time)

            ohlc_data.at[trough_index, 'drawdown_recovery_time'] = recovery_time

            drawdown_in_progress = False

    # Filter out NaN values from drawdowns, durations, and recoveries
    drawdowns = [d for d in drawdowns if not np.isnan(d)]
    durations = [d for d in durations if not np.isnan(d)]
    recoveries = [r for r in recoveries if not np.isnan(r)]

    # Check if there are valid values before computing statistics
    if drawdowns:
        max_drawdown_value = np.max(drawdowns)
        max_drawdown_index = drawdowns.index(max_drawdown_value)
        max_duration = durations[max_drawdown_index]
    else:
        max_drawdown_value = 0.0
        max_duration = 0

    # Calculate summary statistics for drawdowns
    drawdown_summary = {
        'median_drawdown': np.median(drawdowns) if drawdowns else 0.0,
        'average_drawdown': np.mean(drawdowns) if drawdowns else 0.0,
        'max_drawdown': max_drawdown_value,
        'stddev_drawdown': np.std(drawdowns) if drawdowns else 0.0,
        'median_duration': np.median(durations) if durations else 0,
        'average_duration': np.mean(durations) if durations else 0,
        'max_duration': max_duration,
        'stddev_duration': np.std(durations) if durations else 0,
        'median_recovery': np.median(recoveries) if recoveries else 0,
        'average_recovery': np.mean(recoveries) if recoveries else 0,
        'max_recovery': np.max(recoveries) if recoveries else 0,
        'stddev_recovery': np.std(recoveries) if recoveries else 0
    }

    return drawdown_summary, ohlc_data

def calculate_runups(ohlc_data: pd.DataFrame):
    """
    Calculate runups, durations, and recovery times using independent 6-pivot lows and highs.
    Directly updates the ohlc_data DataFrame with row-level runup, duration, and recovery data.
    """
    ohlc_data['runup'] = np.nan
    ohlc_data['runup_duration'] = np.nan
    ohlc_data['runup_recovery_time'] = np.nan

    min_low = ohlc_data['low'].iloc[0]
    min_low_index = 0
    runups = []
    durations = []
    recoveries = []
    runup_in_progress = False
    peak_index = None
    highest_peak = None

    for i in range(1, len(ohlc_data)):
        pivot_low = ohlc_data['6pivotlow'].iloc[i]
        pivot_high = ohlc_data['6pivothigh'].iloc[i]
        current_close = ohlc_data['close'].iloc[i]

        # Check if pivot_low is non-zero before updating min_low
        if not np.isnan(pivot_low) and pivot_low > 0 and pivot_low < min_low:
            min_low = pivot_low
            min_low_index = i
            runup_in_progress = False
            peak_index = None
            highest_peak = None

        # Only calculate runup if min_low is greater than zero
        if not np.isnan(pivot_high) and min_low > 0:
            runup = (pivot_high - min_low) / min_low * 100
            runups.append(runup)

            duration = i - min_low_index
            durations.append(duration)

            ohlc_data.at[i, 'runup'] = runup
            ohlc_data.at[i, 'runup_duration'] = duration

            if highest_peak is None or pivot_high > highest_peak:
                highest_peak = pivot_high
                peak_index = i

            runup_in_progress = True

        if runup_in_progress and current_close < min_low:
            recovery_time = i - peak_index
            recoveries.append(recovery_time)

            ohlc_data.at[peak_index, 'runup_recovery_time'] = recovery_time

            runup_in_progress = False

    # Filter out NaN values from runups, durations, and recoveries
    runups = [r for r in runups if not np.isnan(r)]
    durations = [d for d in durations if not np.isnan(d)]
    recoveries = [r for r in recoveries if not np.isnan(r)]

    # Check if there are valid values before computing statistics
    if runups:
        max_runup_value = np.max(runups)
        max_runup_index = runups.index(max_runup_value)
        max_duration = durations[max_runup_index]
    else:
        max_runup_value = 0.0
        max_duration = 0

    runup_summary = {
        'median_runup': np.median(runups) if runups else 0.0,
        'average_runup': np.mean(runups) if runups else 0.0,
        'max_runup': max_runup_value,
        'stddev_runup': np.std(runups) if runups else 0.0,
        'median_duration': np.median(durations) if durations else 0,
        'average_duration': np.mean(durations) if durations else 0,
        'max_duration': max_duration,
        'stddev_duration': np.std(durations) if durations else 0,
        'median_recovery': np.median(recoveries) if recoveries else 0,
        'average_recovery': np.mean(recoveries) if recoveries else 0,
        'max_recovery': np.max(recoveries) if recoveries else 0,
        'stddev_recovery': np.std(recoveries) if recoveries else 0
    }

    return runup_summary, ohlc_data


def process_ticker_data(ticker, ohlc_data, connection_params):
    """
    Process OHLC data for a specific ticker. Calculate drawdowns and runups, store intermediate
    results and summary statistics in PostgreSQL.
    """
    ohlc_data = ohlc_data[ohlc_data['ticker'] == ticker].copy()

    ohlc_data['date'] = pd.to_datetime(ohlc_data['timestamp']).dt.date
    ohlc_data.drop(columns=['timestamp'], inplace=True)

    ohlc_data = identify_pivot_points(ohlc_data)

    drawdowns_summary, ohlc_data = calculate_drawdowns(ohlc_data)
    runups_summary, ohlc_data = calculate_runups(ohlc_data)

    ohlc_data = ohlc_data[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'openinterest',
                           '6pivothigh', '6pivotlow', 'drawdown', 'drawdown_duration', 'drawdown_recovery_time',
                           'runup', 'runup_duration', 'runup_recovery_time']]

    copy_to_postgres(ohlc_data, 'ohlc_intermediate_calculations', connection_params)

    summary_df = pd.DataFrame({
        'ticker': [ticker],
        'average_drawdown': [drawdowns_summary['average_drawdown']],
        'median_drawdown': [drawdowns_summary['median_drawdown']],
        'max_drawdown': [drawdowns_summary['max_drawdown']],
        'stddev_drawdown': [drawdowns_summary['stddev_drawdown']],
        'average_drawdown_duration': [drawdowns_summary['average_duration']],
        'median_drawdown_duration': [drawdowns_summary['median_duration']],
        'max_drawdown_duration': [drawdowns_summary['max_duration']],
        'stddev_drawdown_duration': [drawdowns_summary['stddev_duration']],
        'average_drawdown_recovery': [drawdowns_summary['average_recovery']],
        'median_drawdown_recovery': [drawdowns_summary['median_recovery']],
        'max_drawdown_recovery': [drawdowns_summary['max_recovery']],
        'stddev_drawdown_recovery': [drawdowns_summary['stddev_recovery']],
        'average_runup': [runups_summary['average_runup']],
        'median_runup': [runups_summary['median_runup']],
        'max_runup': [runups_summary['max_runup']],
        'stddev_runup': [runups_summary['stddev_runup']],
        'average_runup_duration': [runups_summary['average_duration']],
        'median_runup_duration': [runups_summary['median_duration']],
        'max_runup_duration': [runups_summary['max_duration']],
        'stddev_runup_duration': [runups_summary['stddev_duration']],
        'average_runup_recovery': [runups_summary['average_recovery']],
        'median_runup_recovery': [runups_summary['median_recovery']],
        'max_runup_recovery': [runups_summary['max_recovery']],
        'stddev_runup_recovery': [runups_summary['stddev_recovery']]
    })

    copy_to_postgres(summary_df, 'ohlc_summary_statistics', connection_params)

def create_tables_if_not_exist(connection_params):
        """
        Create the intermediate calculations and summary statistics tables if they do not exist.
        """
        # SQL to create the intermediate calculations table
        create_ohlc_intermediate_table = """
        CREATE TABLE IF NOT EXISTS ohlc_intermediate_calculations (
            ticker TEXT,
            date DATE,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            openinterest BIGINT,
            "6pivothigh" DOUBLE PRECISION,
            "6pivotlow" DOUBLE PRECISION,
            drawdown DOUBLE PRECISION,              -- Store individual drawdown values
            drawdown_duration DOUBLE PRECISION,     -- Duration of each drawdown
            drawdown_recovery_time DOUBLE PRECISION,-- Recovery time for each drawdown
            runup DOUBLE PRECISION,                 -- Store individual runup values
            runup_duration DOUBLE PRECISION,        -- Duration of each runup
            runup_recovery_time DOUBLE PRECISION    -- Recovery time for each runup
        );

        -- Add an index on ticker and date to speed up queries
        CREATE INDEX IF NOT EXISTS idx_ticker_date ON ohlc_intermediate_calculations(ticker, date);
        """

        # SQL to create the summary statistics table
        create_ohlc_summary_table = """
        CREATE TABLE IF NOT EXISTS ohlc_summary_statistics (
            ticker TEXT PRIMARY KEY,                -- Each ticker gets one row in this table

            -- Drawdown statistics
            average_drawdown DOUBLE PRECISION,      -- Average drawdown for the ticker
            median_drawdown DOUBLE PRECISION,       -- Median drawdown for the ticker
            max_drawdown DOUBLE PRECISION,          -- Maximum drawdown for the ticker
            stddev_drawdown DOUBLE PRECISION,       -- Standard deviation of drawdowns
            average_drawdown_duration DOUBLE PRECISION, -- Average duration of drawdowns
            median_drawdown_duration DOUBLE PRECISION,  -- Median duration of drawdowns
            max_drawdown_duration DOUBLE PRECISION,     -- Maximum duration of drawdowns
            stddev_drawdown_duration DOUBLE PRECISION,  -- Standard deviation of drawdown durations
            average_drawdown_recovery DOUBLE PRECISION, -- Average recovery time after drawdowns
            median_drawdown_recovery DOUBLE PRECISION,  -- Median recovery time after drawdowns
            max_drawdown_recovery DOUBLE PRECISION,     -- Maximum recovery time after drawdowns
            stddev_drawdown_recovery DOUBLE PRECISION,  -- Standard deviation of recovery times

            -- Runup statistics
            average_runup DOUBLE PRECISION,         -- Average runup for the ticker
            median_runup DOUBLE PRECISION,          -- Median runup for the ticker
            max_runup DOUBLE PRECISION,             -- Maximum runup for the ticker
            stddev_runup DOUBLE PRECISION,          -- Standard deviation of runups
            average_runup_duration DOUBLE PRECISION,    -- Average duration of runups
            median_runup_duration DOUBLE PRECISION,     -- Median duration of runups
            max_runup_duration DOUBLE PRECISION,        -- Maximum duration of runups
            stddev_runup_duration DOUBLE PRECISION,     -- Standard deviation of runup durations
            average_runup_recovery DOUBLE PRECISION,    -- Average recovery time after runups
            median_runup_recovery DOUBLE PRECISION,     -- Median recovery time after runups
            max_runup_recovery DOUBLE PRECISION,        -- Maximum recovery time after runups
            stddev_runup_recovery DOUBLE PRECISION      -- Standard deviation of recovery times for runups
        );
        """

        # Connect to PostgreSQL and execute the table creation statements
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        try:
            # Execute the SQL commands to create the tables
            cursor.execute(create_ohlc_intermediate_table)
            cursor.execute(create_ohlc_summary_table)
            conn.commit()  # Commit changes
        finally:
            cursor.close()
            conn.close()


def main():
    # record start time
    start = time.time()

    connection_params = {
        'host': 'localhost',
        'dbname': 'Plurality',
        'user': 'postgres',
        'password': 'root',
        'port': '5432'
    }

    # Create tables if they don't exist
    create_tables_if_not_exist(connection_params)

    # Ensure the intermediate and summary tables are truncated before inserting new data
    truncate_table('ohlc_intermediate_calculations', connection_params)
    truncate_table('ohlc_summary_statistics', connection_params)

    # Step 1: Fetch all the OHLC data from the database
    query = "SELECT * FROM usstockseod"
    ohlc_data = get_data_from_postgres(query, connection_params)

    # Step 2: Get the list of unique tickers
    tickers = ohlc_data['ticker'].unique()
    total_tickers = len(tickers)

    print(f"Total tickers to process: {total_tickers}")

    # Step 3: Process each ticker and track progress with error handling
    for count, ticker in enumerate(tickers, 1):
        try:
            # Process ticker data
            process_ticker_data(ticker, ohlc_data, connection_params)
        except Exception as e:
            # Log the error and continue with the next ticker
            print(f"Error processing ticker {ticker}: {e}")

        # Step 4: Display progress for every 100 tickers processed
        if count % 100 == 0 or count == total_tickers:
            print(f"Processed {count}/{total_tickers} tickers...")

    end = time.time()
    # print the difference between start
    # and end time in minutes
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")


    sys.exit(0)

if __name__ == "__main__":
    main()

