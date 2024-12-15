import pandas as pd
import psycopg2
import sys


# Function to preprocess the CSV file: remove first row and select columns
def preprocess_csv(file_path, selected_columns, rename_columns):
    # Load CSV file, skip first irrelevant row
    df = pd.read_csv(file_path, skiprows=1, header=0)
    df.columns = df.columns.str.strip()

    # Select specific columns
    try:
        if selected_columns:
            df = df[selected_columns]
    except KeyError as e:
        print("KeyError:", e)
        print("Available columns:", df.columns)
        raise

    # Rename columns
    df.rename(columns=rename_columns, inplace=True)

    return df
# Function to create a PostgreSQL table
def create_table(connection, table_name, df):
    # Create table based on dataframe schema
    cursor = connection.cursor()
    columns = ', '.join([f"{col} TEXT" for col in df.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()

# Function to upload data to PostgreSQL
def upload_to_postgres(connection, table_name, df):
    # Upload data to the specified table
    cursor = connection.cursor()
    for _, row in df.iterrows():
        columns = ', '.join(df.columns)
        values = ', '.join([f"'%s'" % value for value in row])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
        cursor.execute(insert_query)
    connection.commit()
    cursor.close()


def main(csv_file_path, selected_columns, rename_columns, db_params, table_name):
    # Preprocess CSV file
    df = preprocess_csv(csv_file_path, selected_columns, rename_columns)

    # Create PostgreSQL connection
    connection = psycopg2.connect(**db_params)

    # Create table in PostgreSQL
    create_table(connection, table_name, df)

    # Upload data to PostgreSQL
    upload_to_postgres(connection, table_name, df)

    # Close the connection
    connection.close()


if __name__ == "__main__":
    # Parameters
    csv_file_path = r'C:\Users\uvdsa\OneDrive\Desktop\Internals\!NEWLONYA.csv'  # Path to your CSV file
    selected_columns = None  # Specify the columns you want
    rename_columns = {}  # Columns to rename
    db_params = {
        'dbname': 'markets_internals',
        'user': 'postgres',
        'password': 'root',
        'host': 'localhost',
        'port': '5432'
    }  # PostgreSQL connection parameters
    table_name = 'nylows_composite_raw'  # Desired table name in PostgreSQL

    # Run the main process
    main(csv_file_path, selected_columns, rename_columns, db_params, table_name)

    sys.exit(0)



