import psycopg2
import pandas as pd
import time
import sys
import numpy as np


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


def get_true_indicators_dates(conn, ticker, indicators, logic='AND'):
    """ Get all dates where indicators are True for a given ticker based on the specified logic ('AND' or 'OR') """
    try:
        if logic.upper() == 'AND':
            # Create the SQL WHERE clauses for AND logic
            where_clauses = " AND ".join([f'"{indicator}" = TRUE' for indicator in indicators])
        elif logic.upper() == 'OR':
            # Create the SQL WHERE clauses for OR logic
            where_clauses = " OR ".join([f'"{indicator}" = TRUE' for indicator in indicators])
        else:
            raise ValueError("Invalid logic specified. Use 'AND' or 'OR'.")

        # Define the SQL query
        query = f"""
        SELECT date
        FROM key_indicators_alltickers
        WHERE ticker = %s
          AND ({where_clauses})
        ORDER BY date;
        """

        # Execute the query and fetch the data
        df = pd.read_sql(query, conn, params=(ticker,))
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_outputs_for_dates(conn, ticker, dates, output_columns):
    """ Get the return values for a given ticker and set of dates """
    try:
        # Convert dates to a format suitable for SQL IN clause
        formatted_dates = "', '".join(dates)
        formatted_dates = f"('{formatted_dates}')"

        # Define the SQL query to get returns for the specified dates
        query = f"""
        SELECT date, {', '.join(output_columns)}
        FROM key_indicators_alltickers
        WHERE ticker = %s
          AND date IN {formatted_dates}
        ORDER BY date;
        """

        # Execute the query and fetch the data
        df = pd.read_sql(query, conn, params=(ticker,))
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_all_outputs(conn, ticker, output_columns):
    """ Get the return values for all dates for a given ticker """
    try:
        # Define the SQL query to get returns for all dates
        query = f"""
        SELECT date, {', '.join(output_columns)}
        FROM key_indicators_alltickers
        WHERE ticker = %s
        ORDER BY date;
        """

        # Execute the query and fetch the data
        df = pd.read_sql(query, conn, params=(ticker,))
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def create_summary(outputs_df, all_outputs_df, absolute_returns=False):
    """
    Create two summaries: one for boolean columns (with percentages) and one for return value columns.
    Convert string columns to numeric if necessary.
    """

    # Split boolean and return columns
    boolean_columns = outputs_df.select_dtypes(include=['bool']).columns
    return_columns = outputs_df.select_dtypes(include=[np.number]).columns  # Ensure only numeric columns

    # If absolute_returns is True, take the absolute values of return columns
    if absolute_returns:
        outputs_df[return_columns] = outputs_df[return_columns].abs()
        all_outputs_df[return_columns] = all_outputs_df[return_columns].abs()

    # Process boolean columns (percentage of True values)
    total_rows = len(outputs_df)
    total_baseline_rows = len(all_outputs_df)

    boolean_summary = {
        "Metric": ["% True", "% Baseline True"]
    }

    for col in boolean_columns:
        # Calculate the percentage of True values for both the indicator and baseline datasets
        true_percentage = (outputs_df[col].sum() / total_rows) * 100 if total_rows > 0 else 0
        baseline_true_percentage = (all_outputs_df[
                                        col].sum() / total_baseline_rows) * 100 if total_baseline_rows > 0 else 0
        boolean_summary[col] = [f"{true_percentage:.2f}%", f"{baseline_true_percentage:.2f}%"]

    # Create a DataFrame for boolean summary
    summary1_df = pd.DataFrame(boolean_summary)

    # Process return value columns (statistics like mean, median, etc.)
    percentiles = [0.9, 0.8, 0.7, 0.6, 0.5]

    # Updated percentile labels to handle both indicator and baseline values
    indicator_percentile_labels = [f"Top {int(p * 100)}% Mean (Indicator)" for p in percentiles]

    baseline_percentile_labels =  [f"Top {int(p * 100)}% Mean (Baseline)" for p in percentiles]

    return_summary = {
        "Metric": indicator_percentile_labels + ["Mean (Indicator)", "Median (Indicator)", "Standard Deviation (Indicator)"]  + baseline_percentile_labels +
                                       ["Mean (Baseline)", "Median (Baseline)", "Standard Deviation (Baseline)"]
    }


    for col in return_columns:
        column_summary = []
        for p in percentiles:
            top_mean = outputs_df[outputs_df[col] > outputs_df[col].quantile(p)][col].mean()
            column_summary.append(top_mean)

        column_summary.extend([
            outputs_df[col].mean(),
            outputs_df[col].median(),
            outputs_df[col].std()
        ])

        for p in percentiles:
            baseline_top_mean = all_outputs_df[all_outputs_df[col] > all_outputs_df[col].quantile(p)][col].mean()
            column_summary.append(baseline_top_mean)

        column_summary.extend([
            all_outputs_df[col].mean(),
            all_outputs_df[col].median(),
            all_outputs_df[col].std()
        ])
        return_summary[col] = column_summary

    # Create a DataFrame for return value summary
    summary2_df = pd.DataFrame(return_summary)

    return summary1_df, summary2_df


def create_edge_sheets(summary1_df, summary2_df):
    """
    Create two edge sheets: one for boolean columns (Edge1) and one for return value columns (Edge2).
    Edge is calculated as (Indicator Value - Baseline Value) / Baseline Value.
    """

    # Edge for Boolean Columns (Edge1)
    edge1_summary = {"Metric": summary1_df["Metric"].copy()}  # Retain the Metric labels from the summary
    edge1_values = []

    for col in summary1_df.columns[1:]:  # Skip the "Metric" column
        # Indicator value is in the first row, baseline value is in the second row
        indicator_value = float(summary1_df.iloc[0][col].replace('%', ''))  # First row: indicator value
        baseline_value = float(summary1_df.iloc[1][col].replace('%', ''))  # Second row: baseline value

        # Calculate the edge if baseline_value is not 0
        if baseline_value == 0:
            edge1_values.append(np.nan)  # Handle division by zero
        else:
            edge = (indicator_value - baseline_value) / baseline_value
            edge1_values.append(edge)

    # Create the edge1 DataFrame
    edge1_df = pd.DataFrame(
        {"Metric": ["Edge"], **{col: [edge_value] for col, edge_value in zip(summary1_df.columns[1:], edge1_values)}})

    # Edge for Return Value Columns (Edge2)
    metric_names = [
        "Top 90% Mean", "Top 80% Mean", "Top 70% Mean", "Top 60% Mean", "Top 50% Mean",
        "Mean", "Median", "Standard Deviation"
    ]

    edge2_summary = {"Metric": metric_names}  # Metric names will be the row labels
    edge2_values = []

    # Calculate the number of rows for indicator values (first half of the rows)
    num_metrics = len(metric_names)

    # For each metric row (there are num_metrics indicator rows and num_metrics baseline rows)
    for i in range(num_metrics):  # Iterate over the first half of the rows
        row_edges = []  # To store edges for each metric

        for col in summary2_df.columns[1:]:  # Skip the "Metric" column
            indicator_value = summary2_df.iloc[i][col]  # First half: indicator value
            baseline_value = summary2_df.iloc[i + num_metrics][col]  # Second half: baseline value

            # Calculate the edge if baseline_value is not 0
            if pd.isna(indicator_value) or pd.isna(baseline_value) or baseline_value == 0:
                row_edges.append(np.nan)
            else:
                edge = (indicator_value - baseline_value) / baseline_value
                row_edges.append(edge)

        edge2_values.append(row_edges)  # Append the calculated edges for this metric row

    # Create the edge2 DataFrame, using the metrics for the index
    edge2_df = pd.DataFrame(edge2_values, columns=summary2_df.columns[1:], index=metric_names)

    # Add the Metric column explicitly to the edge2_df DataFrame
    edge2_df.insert(0, "Metric", metric_names)

    return edge1_df, edge2_df


def main():
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

    conn = connect_to_db(param_dic)


    if conn is not None:
        # Define the list of indicators to check
        indicators = ['lrlchv']  # Replace with your actual indicator column names

        # Specify the logic ('AND' or 'OR')
        logic = 'AND'  # Change to 'OR' if needed

        # Get the dates where indicators are True for a specific ticker based on the specified logic
        ticker = 'SPY'  # Replace with the ticker of interest
        result_df = get_true_indicators_dates(conn, ticker, indicators, logic)

        if result_df is not None and not result_df.empty:
            # Extract dates from result_df
            dates = result_df['date'].tolist()

            # Specify the return columns you want to fetch
            output_columns = ['return_1day', 'return_2days', 'return_3days', 'return_4days', 'return_1week', 'return_2weeks', 'return_3weeks', 'return_1month',
                              'return_2months', 'return_1quarter', 'return_2quarters', 'return_3quarters', 'return_1year', 'ftd', 'fltd','ftdm1', 'ftdm2', 'ftdm3']  # Add more return columns as needed

            # Get the returns for the specified dates
            outputs_df = get_outputs_for_dates(conn, ticker, dates, output_columns)
            details_df = outputs_df.copy()

            # Get the baseline returns for all dates
            all_outputs_df = get_all_outputs(conn, ticker, output_columns)

            # Specify whether to use absolute returns or actual returns
            absolute_returns = True  # Set to False to use actual returns

            # Create two summaries: one for booleans and one for return values
            summary1_df, summary2_df = create_summary(outputs_df, all_outputs_df, absolute_returns)

            # Create two edge sheets: one for booleans and one for return values
            edge1_df, edge2_df = create_edge_sheets(summary1_df, summary2_df)

            file_path = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\Indicators\perf_analysis.xlsx"

            # Write the results and summary to an Excel file
            with pd.ExcelWriter(file_path) as writer:
                details_df.to_excel(writer, sheet_name="Details", index=False)
                summary1_df.to_excel(writer, sheet_name="Summary1", index=False)  # Boolean summary
                summary2_df.to_excel(writer, sheet_name="Summary2", index=False)  # Return value summary
                edge1_df.to_excel(writer, sheet_name="Edge1", index=False)  # Edge for Boolean columns
                edge2_df.to_excel(writer, sheet_name="Edge2", index=False)  # Edge for Return value columns

            print(f"Excel file 'perf_analysis.xlsx' created with details, summary1, summary2, edge1, and edge2.")
        else:
            print("No data found or an error occurred.")
        # Close the database connection
        conn.close()

        # record end time
        end = time.time()

        # print the difference between start
        # and end time in milli. secs
        print("The time of execution of above program is :",
              ((end - start) * 10 ** 3) / 60000, "minutes")

        sys.exit(0)


if __name__ == '__main__':
    main()
