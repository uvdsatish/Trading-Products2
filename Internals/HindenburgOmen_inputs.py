import pandas as pd
import psycopg2
import sys


# Function to preprocess the CSV file: remove first row and select columns
def preprocess_csv(file_path):
    # Load CSV file, skip first irrelevant row
    df = pd.read_csv(file_path, skiprows=1, header=0)
    df.columns = df.columns.str.strip()

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
    cursor = connection.cursor()

    try:
        # First delete existing data from the table
        delete_query = f"DELETE FROM {table_name};"
        cursor.execute(delete_query)

        # Then insert new data
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            # Using parameterized queries for better security and handling of special characters
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            cursor.execute(insert_query, tuple(row))

        # Commit the transaction
        connection.commit()
        print(f"Successfully deleted old data and uploaded new data to {table_name}")

    except Exception as e:
        connection.rollback()  # Rollback in case of error
        print(f"Error uploading data to {table_name}: {str(e)}")
        raise

    finally:
        cursor.close()




def main(csv_file_path, connection, table_name):
    # Preprocess CSV file
    df = preprocess_csv(csv_file_path)


    # Create table in PostgreSQL
    create_table(connection, table_name, df)

    # Upload data to PostgreSQL
    upload_to_postgres(connection, table_name, df)

    # Close the connection



if __name__ == "__main__":
    # Parameters
    db_params = {
        'dbname': 'markets_internals',
        'user': 'postgres',
        'password': 'root',
        'host': 'localhost',
        'port': '5432'
    }  # PostgreSQL connection parameters

    # Create PostgreSQL connection
    connection = psycopg2.connect(**db_params)

    # Define dictionary mapping CSV files to their corresponding table names
    file_table_mapping = {
        r'C:\Users\uvdsa\OneDrive\Desktop\Internals\!NEWLONYA.csv': 'nylows_composite_raw',
        r'C:\Users\uvdsa\OneDrive\Desktop\Internals\!NEWHINYA.csv': 'nyhighs_composite_raw',
        r'C:\Users\uvdsa\OneDrive\Desktop\Internals\!ADVNYA.csv': 'nyadvances_composite_raw',
        r'C:\Users\uvdsa\OneDrive\Desktop\Internals\!DECLNYA (1).csv': 'nydeclines_composite_raw',
        r'C:\Users\uvdsa\OneDrive\Desktop\Internals\$NYA.csv': 'nyse_composite_raw',
        r'C:\Users\uvdsa\OneDrive\Desktop\Internals\$NYTOT.csv': 'nytotal_composite_raw'

        # Add more mappings as needed
    }


    for csv_file_path, table_name in file_table_mapping.items():
        try:
            print(f"Processing {csv_file_path} into table {table_name}")
            main(csv_file_path, connection, table_name)
            print(f"Successfully processed {table_name}")
        except Exception as e:
            print(f"Error processing {csv_file_path}: {str(e)}")

    connection.close()

    sys.exit(0)



