import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def get_intermediate_data_for_ticker(connection_params, ticker, table_name, start_date=None):
    """
    Fetch the relevant data (date, drawdown, runup) from the specified table for a specific ticker.
    If start_date is provided, fetch data starting from that date.
    """
    if start_date:
        query = f"""
        SELECT date, drawdown, runup
        FROM {table_name}
        WHERE ticker = '{ticker}'
        AND date >= '{start_date}'
        AND (drawdown IS NOT NULL OR runup IS NOT NULL)
        ORDER BY date
        """
    else:
        query = f"""
        SELECT date, drawdown, runup
        FROM {table_name}
        WHERE ticker = '{ticker}'
        AND (drawdown IS NOT NULL OR runup IS NOT NULL)
        ORDER BY date
        """

    conn = psycopg2.connect(**connection_params)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    # Forward-fill NaN values to make the lines continuous
    df[['drawdown', 'runup']] = df[['drawdown', 'runup']].ffill()

    return df


def plot_and_save_line_graph_seaborn(df, ticker, output_file):
    """
    Plot the drawdown and runup data as a clean line graph for a ticker using Seaborn and save the plot as a file.
    """
    # Convert 'date' to a proper datetime format for plotting
    df['date'] = pd.to_datetime(df['date'])

    # Set Seaborn style for the plot
    sns.set(style="whitegrid")

    # Create a figure and axis for the line graph
    plt.figure(figsize=(10, 6))

    # Line plot for drawdowns (purple) using Seaborn without markers
    sns.lineplot(x='date', y='drawdown', data=df, label='Drawdown', color='purple')

    # Line plot for runups (blue) using Seaborn without markers
    sns.lineplot(x='date', y='runup', data=df, label='Runup', color='blue')

    # Add title and labels
    plt.title(f'Drawdowns and Runups for {ticker}', fontsize=14)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Percentage', fontsize=12)

    # Show legend to differentiate between lines
    plt.legend()

    # Save the line graph to a file
    plt.savefig(output_file)

    # Close the plot to avoid display in certain environments (e.g., Jupyter)
    plt.close()


def process_version(connection_params, tickers, table_name, output_file_suffix, start_date=None):
    """
    Process each ticker for the given table name and save the graphs with the provided output file suffix.
    """
    for ticker in tickers:
        # Step 1: Fetch the data for the specific ticker from the given start date
        df = get_intermediate_data_for_ticker(connection_params, ticker, table_name, start_date)

        # Step 2: Define the output file name for the graph, including the suffix to differentiate versions

        output_file = f"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\drawdowns_runups\{ticker}_drawdown_runup_{output_file_suffix}.png"

        # Step 3: Plot the data as a line graph and save to a file using Seaborn
        plot_and_save_line_graph_seaborn(df, ticker, output_file)


def main():
    connection_params = {
        'host': 'localhost',
        'dbname': 'Plurality',
        'user': 'postgres',
        'password': 'root',
        'port': '5432'
    }

    # Specify the list of tickers
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'VITL', 'CAVA', 'PTON']  # Replace with your list of tickers

    # Optional start date (set this to None if you want to fetch all data)
    start_date = '2020-01-01'  # Replace with the desired start date, or None to fetch all data

    # Version 1: Process using ohlc_intermediate_calculations
    table_name_v1 = 'ohlc_intermediate_calculations'
    output_suffix_v1 = 'version_1'
    process_version(connection_params, tickers, table_name_v1, output_suffix_v1, start_date)

    # Version 2: Process using ohlc_intermediate_calculations_v2
    table_name_v2 = 'ohlc_intermediate_calculations_v2'
    output_suffix_v2 = 'version_2'
    process_version(connection_params, tickers, table_name_v2, output_suffix_v2, start_date)


if __name__ == "__main__":
    main()
