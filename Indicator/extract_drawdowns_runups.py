import psycopg2
import pandas as pd
import time
import sys


# Function to connect to PostgreSQL
def connect_to_db(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None
    print("Connection successful")
    return conn


# Function to fetch data for a specific ticker from usstocksdd
def fetch_data_for_ticker(conn, ticker):
    cursor = conn.cursor()
    # Fetch the data for the given ticker (assuming the ticker is a column in usstocksdd)
    query = '''
        SELECT date, max_drawdown, max_runup, drawdown_duration, runup_duration 
        FROM usstocksdd
        WHERE ticker = %s
    '''
    cursor.execute(query, (ticker,))
    rows = cursor.fetchall()
    # Convert the result to a DataFrame
    data = pd.DataFrame(rows, columns=['date', 'max_drawdown', 'max_runup', 'drawdown_duration', 'runup_duration'])
    return data


# Function to calculate statistics for each column
def calculate_statistics(data, column_name):
    # Filter out NaN values for clean statistics calculation
    clean_data = data[column_name].dropna()

    if len(clean_data) == 0:
        return None  # No valid data for the given column

    stats = {
        'occurrences': len(clean_data),
        'average': clean_data.mean(),
        'maximum': clean_data.max(),
        'minimum': clean_data.min(),
        'median': clean_data.median(),
        'standard_deviation': clean_data.std()
    }
    return stats


# Function to get statistics for a ticker and return a dictionary
def get_statistics_for_ticker(conn, ticker):
    # Fetch data for the ticker
    data = fetch_data_for_ticker(conn, ticker)

    if data.empty:
        return None

    # Calculate statistics for longs (drawdowns)
    long_drawdown_stats = calculate_statistics(data, 'max_drawdown')
    long_duration_stats = calculate_statistics(data, 'drawdown_duration')

    # Calculate statistics for shorts (run-ups)
    short_runup_stats = calculate_statistics(data, 'max_runup')
    short_duration_stats = calculate_statistics(data, 'runup_duration')

    if long_drawdown_stats and short_runup_stats:
        # Combine all statistics into a dictionary for the given ticker
        stats = {
            'ticker': ticker,
            'long_occurrences': long_drawdown_stats['occurrences'],
            'avg_long_drawdown': long_drawdown_stats['average'],
            'max_long_drawdown': long_drawdown_stats['maximum'],
            'min_long_drawdown': long_drawdown_stats['minimum'],
            'median_long_drawdown': long_drawdown_stats['median'],
            'stddev_long_drawdown': long_drawdown_stats['standard_deviation'],
            'avg_long_duration': long_duration_stats['average'],
            'median_long_duration': long_duration_stats['median'],

            'short_occurrences': short_runup_stats['occurrences'],
            'avg_short_runup': short_runup_stats['average'],
            'max_short_runup': short_runup_stats['maximum'],
            'min_short_runup': short_runup_stats['minimum'],
            'median_short_runup': short_runup_stats['median'],
            'stddev_short_runup': short_runup_stats['standard_deviation'],
            'avg_short_duration': short_duration_stats['average'],
            'median_short_duration': short_duration_stats['median']
        }
        return stats
    else:
        return None


# Function to process multiple tickers and return a DataFrame with the results
def process_tickers(conn, tickers):
    results = []

    for ticker in tickers:
        stats = get_statistics_for_ticker(conn, ticker)
        if stats:
            results.append(stats)

    # Convert the results to a DataFrame
    df = pd.DataFrame(results)
    return df


# Function to export the DataFrame to an Excel file
def export_to_excel(df, file_name='ticker_statistics.xlsx'):
    df.to_excel(file_name, index=False)
    print(f"Data exported to {file_name}")


# Main function to handle the process for multiple tickers
def main(tickers, output_file):
    start = time.time()

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    conn = connect_to_db(param_dic)

    # Process all the tickers and get a DataFrame of results
    df = process_tickers(conn, tickers)

    # Close the connection
    conn.close()

    # Export the DataFrame to an Excel file
    export_to_excel(df, output_file)

    end = time.time()
    # print the difference between start
    # and end time in minutes
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")

    sys.exit(0)


# Example usage
if __name__ == '__main__':

    # List of tickers
    tickers = ['AAP', 'MSFT', 'GOOGL', 'NVDA']  # Example list of tickers
    output_file = f"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\Indicators\drawdowns.xlsx"
    main(tickers, output_file)
