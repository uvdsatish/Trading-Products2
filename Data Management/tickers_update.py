import csv
import psycopg2



# Database connection setup
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="Plurality",
            user="postgres",
            password="root",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


# Function to update the table with data from CSV
def update_table_from_csv(csv_file_path, conn):
    cursor = conn.cursor()

    # Open CSV file
    with open(csv_file_path, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)

        # Loop through each row in the CSV
        for row in reader:
            industry = row['industry']
            ticker = row['ticker']
            name = row['name']
            sector = row['sector']
            volume = int(row['volume'])
            market_cap = float(row['marketcap'])

            # Upsert query: If ticker exists, update; if not, insert
            upsert_query = """
            INSERT INTO iblkupall (industry, ticker, name, sector, volume, marketcap)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) 
            DO UPDATE SET
                industry = EXCLUDED.industry,
                name = EXCLUDED.name,
                sector = EXCLUDED.sector,
                volume = EXCLUDED.volume,
                marketcap = EXCLUDED.marketcap;
            """

            # Execute the query with the row data
            cursor.execute(upsert_query, (industry, ticker, name, sector, volume, market_cap))

    # Commit the transaction and close the cursor
    conn.commit()
    cursor.close()


# Main program execution
if __name__ == "__main__":
    # Define the path to your CSV file
    csv_file_path = r"D:\DBBackup\iblkupall_d09152024.csv"

    # Connect to the PostgreSQL database
    conn = connect_db()

    if conn:
        # Update the table with data from the CSV
        update_table_from_csv(csv_file_path, conn)

        # Close the database connection
        conn.close()
        print("Data update complete!")
    else:
        print("Failed to connect to the database.")
