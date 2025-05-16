import pandas as pd
import psycopg2
from typing import List, Dict, Tuple
from dataclasses import dataclass
from datetime import datetime
from typing import List
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration class for model parameters"""
    results_table: str  # Table containing model results
    date_column: str  # Name of date column
    signal_column: str  # Name of signal column (e.g., 'hindenburg_omen')
    signal_value: any  # Value that indicates a signal (e.g., True)
    returns_table: str  # Table containing returns data
    ticker_column: str
    perf_ticker: str
    return_columns: List[str]  # List of return columns to analyze


def get_internals_database_connection():
    """
    Create and return a database connection.
    Update these parameters according to your database configuration.
    """

    try:
        conn = psycopg2.connect(
            dbname='markets_internals',
            user='postgres',
            password='root',
            host='localhost',
            port='5432'
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def get_technicals_database_connection():
    """
    Create and return a database connection.
    Update these parameters according to your database configuration.
    """

    try:
        conn = psycopg2.connect(
            dbname='markets_technicals',
            user='postgres',
            password='root',
            host='localhost',
            port='5432'
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise



def get_signal_dates(config: ModelConfig) -> List[str]:
    """
    Fetch dates where the model signal is true from the database.

    Args:
        config (ModelConfig): Configuration object containing model parameters

    Returns:
        List[str]: List of dates in string format (YYYY-MM-DD)
    """
    try:
        conn = get_internals_database_connection()

        # Handle different types of signal values
        if isinstance(config.signal_value, bool):
            signal_condition = f"{config.signal_column} IS {config.signal_value}"
        elif isinstance(config.signal_value, (int, float)):
            signal_condition = f"{config.signal_column} = {config.signal_value}"
        else:
            signal_condition = f"{config.signal_column} = '{config.signal_value}'"

        query = f""" 
            SELECT {config.date_column} 
            FROM {config.results_table} 
            WHERE {signal_condition} 
            ORDER BY {config.date_column};
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            raw_dates = cursor.fetchall()

            # Handle date formatting, accounting for both datetime and string types
            dates = []
            for row in raw_dates:
                date_value = row[0]
                if hasattr(date_value, 'strftime'):
                    # If it's already a datetime object
                    formatted_date = date_value.strftime('%Y-%m-%d')
                else:
                    try:
                        # If it's a string, try to parse it as a datetime
                        # Adjust the input format string based on your actual date format
                        date_obj = datetime.strptime(str(date_value), '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        # If parsing fails, use the string as-is
                        formatted_date = str(date_value)
                dates.append(formatted_date)

        conn.close()
        logger.info(f"Retrieved {len(dates)} signal dates from {config.results_table}")
        return dates

    except Exception as e:
        logger.error(f"Error fetching signal dates: {str(e)}")
        raise
def get_returns(dates: List[str], config: ModelConfig) -> Tuple[pd.DataFrame, Dict]:
    """
    Fetch return values for the given dates from the database.

    Args:
        dates (List[str]): List of dates to fetch returns for
        config (ModelConfig): Configuration object containing model parameters

    Returns:
        Tuple[pd.DataFrame, Dict]: DataFrame containing dates and returns, and summary statistics
    """
    try:
        conn = get_technicals_database_connection()
        dates_str = "','".join(dates)

        # Construct column list for query
        columns_str = f"{config.date_column}, {config.ticker_column}, " + ", ".join(config.return_columns)

        query = f"""
            SELECT {columns_str}
            FROM {config.returns_table}
            WHERE {config.date_column} IN ('{dates_str}') and {config.ticker_column} = '{config.perf_ticker}'
            ORDER BY {config.date_column};
        """

        returns_df = pd.read_sql_query(query, conn)
        conn.close()

        logger.info(f"Retrieved returns for {len(returns_df)} dates")

        # Convert date column to datetime
        returns_df[config.date_column] = pd.to_datetime(returns_df[config.date_column])

        # Calculate summary statistics
        summary_stats = {
            'count': len(returns_df),
            'mean_returns': {
                col: returns_df[col].mean()
                for col in config.return_columns
            },
            'median_returns': {
                col: returns_df[col].median()
                for col in config.return_columns
            },
            'std_returns': {
                col: returns_df[col].std()
                for col in config.return_columns
            },
            'positive_returns': {
                col: (returns_df[col] > 0).mean()
                for col in config.return_columns
            },
            'negative_returns': {
                col: (returns_df[col] < 0).mean()
                for col in config.return_columns
            }
        }

        logger.info("Calculated summary statistics")

        return returns_df, summary_stats

    except Exception as e:
        logger.error(f"Error fetching returns: {str(e)}")
        raise


def analyze_model(config: ModelConfig) -> Tuple[pd.DataFrame, Dict]:
    """
    Main function to run the analysis for any given model configuration.

    Args:
        config (ModelConfig): Configuration object containing model parameters

    Returns:
        Tuple[pd.DataFrame, Dict]: Returns DataFrame and summary statistics
    """
    try:
        # Get signal dates
        signal_dates = get_signal_dates(config)
        logger.info(f"Found {len(signal_dates)} signal dates")

        # Get corresponding returns
        returns_df, summary_stats = get_returns(signal_dates, config)

        # Print summary statistics
        print(f"\nAnalysis Results for {config.results_table}")
        print(f"Total signal occurrences: {summary_stats['count']}")

        print("\nMean Returns:")
        for period, value in summary_stats['mean_returns'].items():
            print(f"{period}: {value:.2%}")

        print("\nMedian Returns:")
        for period, value in summary_stats['median_returns'].items():
            print(f"{period}: {value:.2%}")

        print("\nPositive Return Probability:")
        for period, value in summary_stats['positive_returns'].items():
            print(f"{period}: {value:.2%}")

        print("\nNegative Return Probability:")
        for period, value in summary_stats['negative_returns'].items():
            print(f"{period}: {value:.2%}")

        return returns_df, summary_stats

    except Exception as e:
        logger.error(f"Error in analyze_model: {str(e)}")
        raise


if __name__ == "__main__":
    # Example usage for Hindenburg Omen
    hindenburg_config = ModelConfig(
        results_table="hindenburg_omen_results",
        date_column="date",
        signal_column="hindenburg_omen",
        signal_value=True,
        returns_table="key_indicators_alltickers",
        ticker_column="ticker",
        perf_ticker="SPY",
        return_columns=[
            "return_1day",
            "return_2days",
            "return_3days",
            "return_1week",
            "return_2weeks",
            "return_3weeks",
            "return_1month",
            "return_2months",
            "return_1quarter",
            "return_2quarters",
            "return_3quarters",
            "return_1year"
        ]
    )
    # Run analysis
    returns_df, stats = analyze_model(hindenburg_config)

    sys.exit(0)

    # # Example usage for another model (e.g., Moving Average Crossover)
    # ma_crossover_config = ModelConfig(
    #     results_table="ma_crossover_signals",
    #     date_column="signal_date",
    #     signal_column="signal_type",
    #     signal_value="BUY",
    #     returns_table="key_indicator_alltickers",
    #     return_columns=[
    #         "return_1day",
    #         "return_2days",
    #         "return_3days",
    #         "return_1week",
    #         "return_2weeks"
    #     ]
    # )
    #
    # # Run analysis for second model
    # ma_returns_df, ma_stats = analyze_model(ma_crossover_config)