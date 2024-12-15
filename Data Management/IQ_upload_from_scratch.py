import psycopg2
import socket
import csv
from io import StringIO
import time
import iqfeedTest as iq


# PostgreSQL connection setup
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


# Function to fetch unprocessed tickers in chunks of 100
def fetch_unprocessed_tickers(conn, chunk_size):
    cursor = conn.cursor()
    # Query to find tickers in iblkupall but not yet in usstockseod
    query = f"""
    SELECT ticker FROM iblkupall 
    WHERE ticker NOT IN (SELECT DISTINCT ticker FROM usstockseod)
    LIMIT {chunk_size};
    """
    cursor.execute(query)
    tickers = cursor.fetchall()
    return [ticker[0] for ticker in tickers]


# Function to connect to IQFeed and get OHLC data for a list of tickers
def fetch_ohlc_from_iqfeed(ticker_list):
    host = '127.0.0.1'
    port = 9100  # Change to appropriate IQFeed port if different
    ohlc_data = []

    try:
        # Open a socket connection to IQFeed
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        for ticker in ticker_list:
            # Request OHLC data for the ticker
            request = f"HDT,{ticker},19000101,,0\r\n"
            sock.sendall(request.encode())

            response = ''
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                response += data.decode()

            # Parse the response (usually CSV format)
            reader = csv.reader(response.splitlines())
            for row in reader:
                # Assuming CSV columns: Ticker, Timestamp, High, Low, Open, Close, Volume, OpenInterest
                if len(row) == 8:
                    ohlc_data.append({
                        'ticker': row[0],
                        'timestamp': row[1],  # Timestamp in string format
                        'high': float(row[2]),
                        'low': float(row[3]),
                        'open': float(row[4]),
                        'close': float(row[5]),
                        'volume': int(row[6]),
                        'openinterest': int(row[7])
                    })
    except Exception as e:
        print(f"Error fetching data for tickers: {ticker_list}: {e}")
        return None
    finally:
        sock.close()

    return ohlc_data


# Function to store OHLC data into PostgreSQL using COPY
def store_ohlc_data_using_copy(conn, ohlc_data):
    if not ohlc_data:
        return

    # Create an in-memory file using StringIO
    f = StringIO()

    # Prepare the data in CSV format for COPY
    for row in ohlc_data:
        f.write(
            f"{row['ticker']},{row['timestamp']},{row['high']},{row['low']},{row['open']},{row['close']},{row['volume']},{row['openinterest']}\n")

    # Move the cursor to the beginning of the StringIO object
    f.seek(0)

    cursor = conn.cursor()

    # Use COPY command to bulk load data into PostgreSQL
    cursor.copy_expert("""
        COPY usstockseod (ticker, timestamp, high, low, open, close, volume, openinterest)
        FROM STDIN WITH (FORMAT csv)
    """, f)

    # Commit the transaction
    conn.commit()


# Main program execution
if __name__ == "__main__":
    # Connect to PostgreSQL database
    conn = connect_db()

    iq.launch_service()

    if conn:
        chunk_size = 500
        # Keep processing until no more tickers are left to process
        while True:
            # Fetch unprocessed tickers in chunks
            tickers = fetch_unprocessed_tickers(conn, chunk_size)
            if not tickers:
                print("All tickers processed!")
                break

            # Fetch OHLC data for the chunk of tickers
            print(f"Fetching data for tickers: {tickers}")
            ohlc_data = fetch_ohlc_from_iqfeed(tickers)

            if ohlc_data:
                # Store OHLC data in PostgreSQL using COPY
                print(f"Storing data for {len(ohlc_data)} records")
                store_ohlc_data_using_copy(conn, ohlc_data)
                print(f"Chunk of {chunk_size} tickers processed.")
            else:
                print("Error fetching data for this chunk, skipping to next.")

            # Pause to avoid overwhelming the system or IQFeed rate limits
            time.sleep(2)

        # Close the database connection
        conn.close()
        print("Data fetching and storing complete!")
    else:
        print("Failed to connect to the database.")
