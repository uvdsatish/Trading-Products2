import pandas as pd
import psycopg2
import sys
import yaml
from typing import Dict, Any, Optional

class DataLoader:
    def __init__(self, config_path: str):
        """Initialize DataLoader with configuration file path."""
        self.config = self._load_config(config_path)
        self.connection = self._create_db_connection()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def _create_db_connection(self) -> psycopg2.extensions.connection:
        """Create database connection using configuration parameters."""
        try:
            return psycopg2.connect(**self.config['database'])
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def preprocess_csv(self, file_path: str, preprocessing_config: Dict) -> pd.DataFrame:
        """
        Preprocess CSV file based on configuration.

        Args:
            file_path: Path to CSV file
            preprocessing_config: Dictionary containing preprocessing parameters
        """
        try:
            df = pd.read_csv(
                file_path,
                skiprows=preprocessing_config.get('skiprows', 0),
                header=preprocessing_config.get('header', 0),
                usecols=preprocessing_config.get('usecols', None),
                dtype=preprocessing_config.get('dtype', None),
                na_values=preprocessing_config.get('na_values', None)
            )

            # Apply column transformations if specified
            if preprocessing_config.get('strip_columns', True):
                df.columns = df.columns.str.strip()

            # Apply custom transformations if defined
            if 'transformations' in preprocessing_config:
                for transform in preprocessing_config['transformations']:
                    if transform['type'] == 'rename_columns':
                        df = df.rename(columns=transform['mapping'])
                    elif transform['type'] == 'drop_columns':
                        df = df.drop(columns=transform['columns'])
                    # Add more transformation types as needed

            return df
        except Exception as e:
            raise ValueError(f"Error preprocessing file {file_path}: {str(e)}")

    def create_table(self, table_name: str, df: pd.DataFrame, table_config: Optional[Dict] = None) -> None:
        """Create PostgreSQL table based on DataFrame schema and configuration."""
        cursor = self.connection.cursor()
        try:
            if table_config and 'schema' in table_config:
                # Use custom schema if provided
                columns = table_config['schema']
            else:
                # Generate schema from DataFrame
                columns = ', '.join([f"{col} TEXT" for col in df.columns])

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns}
                );
            """
            cursor.execute(create_table_query)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Error creating table {table_name}: {str(e)}")
        finally:
            cursor.close()

    def upload_to_postgres(self, table_name: str, df: pd.DataFrame) -> None:
        """Upload data to PostgreSQL table."""
        cursor = self.connection.cursor()
        try:
            # Delete existing data
            cursor.execute(f"DELETE FROM {table_name};")

            # Insert new data
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

            # Batch insert for better performance
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                cursor.executemany(insert_query, batch.values.tolist())

            self.connection.commit()
            print(f"Successfully uploaded data to {table_name}")
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Error uploading data to {table_name}: {str(e)}")
        finally:
            cursor.close()

    def process_files(self) -> None:
        """Process all files specified in configuration."""
        for file_config in self.config['files']:
            try:
                file_path = file_config['path']
                table_name = file_config['table_name']
                preprocessing_config = file_config.get('preprocessing', {})
                table_config = file_config.get('table_config', {})

                print(f"Processing {file_path}")
                df = self.preprocess_csv(file_path, preprocessing_config)
                self.create_table(table_name, df, table_config)
                self.upload_to_postgres(table_name, df)
                print(f"Successfully processed {table_name}")
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")

    def __del__(self):
        """Close database connection when object is destroyed."""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()


def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <config_file_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    loader = DataLoader(config_path)
    loader.process_files()


if __name__ == "__main__":
    main()

    sys.exit(0)