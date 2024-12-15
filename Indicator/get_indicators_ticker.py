import psycopg2
import pandas as pd


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


def get_boolean_columns(conn):
    """ Get all boolean columns in the key_indicators_alltickers table """
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'key_indicators_alltickers'
      AND table_schema = 'public'
      AND data_type = 'boolean';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    boolean_columns = [row[0] for row in cursor.fetchall()]
    return boolean_columns


def get_latest_date_for_ticker(conn, ticker):
    """ Get the latest date for a given ticker in the key_indicators_alltickers table """
    query = """
    SELECT MAX(date) 
    FROM key_indicators_alltickers
    WHERE ticker = %s;
    """
    cursor = conn.cursor()
    cursor.execute(query, (ticker,))
    latest_date = cursor.fetchone()[0]
    return latest_date


def get_true_columns_for_latest_date(conn, tickers):
    """ Get all boolean columns (indicators) that are True for the latest date for the specified ticker(s) """
    try:
        # Get only boolean columns
        boolean_columns = get_boolean_columns(conn)

        results = []

        for ticker in tickers:
            latest_date = get_latest_date_for_ticker(conn, ticker)

            if latest_date is None:
                print(f"No data found for ticker {ticker}.")
                continue

            # Create SQL CASE statements to check for True values
            case_statements = ",\n".join(
                [f"MAX(CASE WHEN \"{col}\" = TRUE THEN 1 ELSE 0 END) AS \"{col}\"" for col in boolean_columns]
            )

            # Define the SQL query
            query = f"""
            SELECT ticker,
                   {case_statements}
            FROM key_indicators_alltickers
            WHERE ticker = %s AND date = %s
            GROUP BY ticker;
            """

            # Execute the query and fetch the data
            df = pd.read_sql(query, conn, params=(ticker, latest_date))

            if not df.empty:
                true_columns_df = df.loc[:, (df != 0).any(axis=0)]
                results.append(true_columns_df)

        if results:
            final_df = pd.concat(results, ignore_index=True)
            return final_df
        else:
            return pd.DataFrame()  # Return empty DataFrame if no results

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def main():
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
        # Define the ticker or list of tickers
        tickers = ['ACIW', 'ALL', 'CAVA', 'CLBT', 'DORM', 'FIS', 'IOT', 'MCY', 'OHI', 'ONON', 'PFSI', 'PGR', 'PPC', 'RKT', 'SKWD', 'WELL', 'SPY']  # Replace with your desired tickers

        # Get the columns (indicators) that are True for the latest date for the specified ticker(s)
        true_columns_df = get_true_columns_for_latest_date(conn, tickers)
        file_path = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\Indicators\true_indicators.xlsx"

        if true_columns_df is not None and not true_columns_df.empty:
            # Write the result to an Excel file
            true_columns_df.to_excel(file_path, index=False)
            print(
                f"Excel file 'true_indicators_latest_date.xlsx' created with the True indicators for the latest date for the specified ticker(s).")
        else:
            print("No True indicators found for the latest date for the specified ticker(s) or an error occurred.")

        # Close the database connection
        conn.close()


if __name__ == '__main__':
    main()
