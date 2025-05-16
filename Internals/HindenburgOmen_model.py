import pandas as pd
import psycopg2
import sys


# Function to get data from PostgreSQL table
def get_data_from_table(db_params, table_name, selected_columns, rename_columns):
    connection = psycopg2.connect(**db_params)
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, connection)
    df = df[selected_columns]
    # Rename columns
    df.rename(columns=rename_columns, inplace=True)

    df['date'] = pd.to_datetime(df['date'].str.strip(), errors='coerce')

    # Convert numeric text columns to appropriate types
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = pd.to_numeric(df[col].str.strip(), errors='coerce')

    connection.close()
    return df


# Function to calculate Hindenburg Omen
def calculate_hindenburg_omen(merged_data):

    # Condition 1: Calculate % of new highs and new lows
    nh_percentage = (merged_data['nh'] / merged_data['total_stocks']) * 100
    nl_percentage = (merged_data['nl'] / merged_data['total_stocks']) * 100
    decent_proportion = (nh_percentage > 2.8) & (nl_percentage > 2.8)

    # Condition 2: Market index is rising (using NYSE Composite Index)
    market_rising = merged_data['close'] > merged_data['close'].shift(50)

    # Condition 3: New highs are not more than twice the new lows
    high_low_ratio = merged_data['nh'] <= (2 * merged_data['nl'])

    # Condition 4: McClellan Oscillator must be negative
    ratio_adjusted_net_advances = ((merged_data['adv'] - merged_data['dcl']) / (
            merged_data['adv'] + merged_data['dcl']))*1000
    ema_19 = ratio_adjusted_net_advances.ewm(span=19, adjust=False).mean()
    ema_39 = ratio_adjusted_net_advances.ewm(span=39, adjust=False).mean()
    mcclellan_oscillator = ema_19 - ema_39
    mco_neg = mcclellan_oscillator < 0

    # Combine all conditions
    hindenburg_omen = decent_proportion & market_rising & high_low_ratio & mco_neg

    result_df = pd.DataFrame({
        'Timestamp': merged_data['date'],
        'date': merged_data['date'].dt.date,
        'NYA_price':merged_data['close'],
        'nh': merged_data['nh'],
        'nl': merged_data['nl'],
        'total': merged_data['total_stocks'],
        'NH_Percentage': nh_percentage,
        'NL_Percentage': nl_percentage,
        'McClellan_Oscillator': mcclellan_oscillator,
        'decent_proportion': decent_proportion,
        'market_rising': market_rising,
        'high_low_ratio': high_low_ratio,
        'mco_neg': mco_neg,
        'Hindenburg_Omen': hindenburg_omen
    }).drop_duplicates(subset=['date'])

    return result_df


# Function to save results to PostgreSQL
def save_results_to_table(db_params, result_df, result_table):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    # Create table based on result_df schema
    columns = ', '.join([f"{col} TIMESTAMP" if col == 'Timestamp' else (f"{col} DATE" if col == 'Date' else (f"{col} FLOAT" if result_df[col].dtype == 'float64' else (f"{col} BOOLEAN" if result_df[col].dtype == 'bool' else (f"{col} INTEGER" if result_df[col].dtype == 'int64' else f"{col} TEXT")))) for col in result_df.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {result_table} (
        {columns}
    ,
PRIMARY KEY (Date)
);
    """
    cursor.execute(create_table_query)
    connection.commit()

    # Insert data into the table
    for _, row in result_df.iterrows():
        columns = ', '.join(result_df.columns)
        values = ', '.join(['%s' if pd.notna(value) else 'NULL' for value in row])
        insert_query = f"INSERT INTO {result_table} ({columns}) VALUES ({values}) ON CONFLICT (Date) DO NOTHING;"
        cursor.execute(insert_query, tuple(row))
    connection.commit()

    cursor.close()
    connection.close()

# Main function to get data, calculate Hindenburg Omen, and save results
def main(db_params, nyse_table, nh_table, nl_table, advances_table, declines_table, total_stocks_table, result_table):
    selected_columns = ['date', 'close']
    rename_columns = {}
    nyse_data = get_data_from_table(db_params, nyse_table, selected_columns, rename_columns)
    selected_columns = ['date', 'close']
    rename_columns = {'close':'total_stocks'}
    total_stocks_data = get_data_from_table(db_params, total_stocks_table, selected_columns, rename_columns)
    selected_columns = ['date', 'close']
    rename_columns = {'close': 'nh'}
    nh_data = get_data_from_table(db_params, nh_table, selected_columns, rename_columns)
    selected_columns = ['date', 'close']
    rename_columns = {'close': 'nl'}
    nl_data = get_data_from_table(db_params, nl_table, selected_columns, rename_columns)
    selected_columns = ['date', 'close']
    rename_columns = {'close': 'adv'}
    advances_data = get_data_from_table(db_params, advances_table, selected_columns, rename_columns)
    selected_columns = ['date', 'close']
    rename_columns = {'close': 'dcl'}
    declines_data = get_data_from_table(db_params, declines_table, selected_columns, rename_columns)

    merged_data = nyse_data.merge(total_stocks_data, on='date', suffixes=('_nyse', '_total')).merge(nh_data,
                                                                                                    on='date').merge(
        nl_data, on='date').merge(advances_data, on='date').merge(declines_data, on='date')
    
    result_df = calculate_hindenburg_omen(merged_data)


    save_results_to_table(db_params, result_df, result_table)

    print("Hindenburg Omen Signal saved to table:")


if __name__ == "__main__":
    # PostgreSQL connection parameters
    db_params = {
        'dbname': 'markets_internals',
        'user': 'postgres',
        'password': 'root',
        'host': 'localhost',
        'port': '5432'
    }

    # Table names
    nyse_table = 'nyse_composite_raw'
    total_stocks_table = 'nytotal_composite_raw'
    nh_table = 'nyhighs_composite_raw'
    nl_table = 'nylows_composite_raw'
    advances_table = 'nyadvances_composite_raw'
    declines_table = 'nydeclines_composite_raw'
    result_table = 'hindenburg_omen_results'

    # Run the main process
    main(db_params, nyse_table, nh_table, nl_table, advances_table, declines_table, total_stocks_table, result_table)

    sys.exit(0)
