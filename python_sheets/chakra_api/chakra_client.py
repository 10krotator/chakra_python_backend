from chakra_py import Chakra
import pandas as pd
from typing import Optional
from dotenv import load_dotenv
import os
import sys
import logging

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_root)

from python_sheets.models.search import SQLQueryGenerator

class ChakraClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        # Only initialize once
        if not self._initialized:
            try:
                # Load environment variables
                load_dotenv()

                # Get session key from environment variables
                session_key = os.getenv('CHAKRA_DB_SESSION_KEY')
                if not session_key:
                    raise ValueError("CHAKRA_DB_SESSION_KEY not found in environment variables")

                self.client = Chakra(session_key)
                self.client.login()

                # Initialize SQL query generator
                self.query_generator = SQLQueryGenerator()
                self._initialized = True

                logging.info("ChakraClient initialized successfully")

            except Exception as e:
                logging.error(f"Failed to initialize ChakraClient: {str(e)}")
                raise

    def query_data(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Query data from specified table
        """
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"

            logging.info(f"Executing query: {query}")
            return self.client.execute(query)

        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise

    def push_data(self, table_name: str, data: pd.DataFrame, create_if_missing: bool = True) -> None:
        """
        Push DataFrame to specified table with proper data type handling
        """
        try:
            # Clean column names: replace spaces with underscores and remove special characters
            data.columns = data.columns.str.replace(' ', '_').str.replace('[^0-9a-zA-Z_]', '')
            # Convert all object/string columns to text type
            for col in data.select_dtypes(include=['object']):
                data[col] = data[col].astype(str)

            # Convert any NaN/None values to empty strings
            data = data.fillna('')

            # Basic push without dtype_overrides
            self.client.push(
                table_name,
                data,
                create_if_missing=create_if_missing
            )

        except Exception as e:
            print(f"Error pushing data: {e}")
            raise

    def parquet_to_pandas(self, file_path: str, columns: Optional[list] = None) -> pd.DataFrame:
        """
        Convert parquet file to pandas DataFrame
        """
        try:
            df = pd.read_parquet(
                file_path,
                columns=columns,
                engine='pyarrow'
            )
            return df
        except Exception as e:
            print(f"Error reading parquet file: {e}")
            raise

    def generate_sql_query(self, question: str) -> str:
        """
        Generate a SQL query from a natural language question

        Args:
            question (str): Natural language question about the database

        Returns:
            str: Generated SQL query
        """
        print(self.query_generator.generate_query(question))
        return self.query_generator.generate_query(question)

    def execute_natural_query(self, question: str) -> tuple[pd.DataFrame, str]:
        """
        Execute a natural language query against the database

        Returns:
            tuple: (DataFrame with results, SQL query string)
        """
        sql_query = self.generate_sql_query(question)
        logging.info(f"Generated SQL query: {sql_query}")
        df = self.client.execute(sql_query)
        return df, sql_query

def load_profiles_to_db():
    """
    Load LinkedIn profiles from parquet to database
    """
    try:
        chakra = ChakraClient()
        
        # Example of loading specific columns
        columns = [
            "FirstName", "LastName", "Headline", "Location",
            "About Me", "Experience", "Education", "Skills", "Certifications", "Recommendations"
        ]
        
        parquet_file = "python_sheets/data/train-00000-of-00002.parquet"
        df_from_parquet = chakra.parquet_to_pandas(parquet_file, columns)
        chakra.push_data("linkedin_profiles", df_from_parquet)

        print("Successfully loaded profiles to database")
        return df_from_parquet

    except Exception as e:
        print(f"Error loading profiles: {e}")
        raise

def query_profiles(limit: int = 5):
    """
    Query profiles from database
    """
    try:
        chakra = ChakraClient()
        df = chakra.query_data("linkedin_profiles", limit=limit)
        print(f"\nQueried {limit} profiles:")
        print(df.head())
        return df

    except Exception as e:
        print(f"Error querying profiles: {e}")
        raise

def execute_natural_query(question: str) -> pd.DataFrame:
    """
    Execute a natural language query against the database
    """
    chakra = ChakraClient()
    sql_query = chakra.generate_sql_query(question)
    df = chakra.client.execute(sql_query)
    print(df)
    return df

# Example usage:
if __name__ == "__main__":
    try:

        # Or run specific functions based on arguments
        import sys
        if len(sys.argv) > 1:
            if sys.argv[1] == "load":
                load_profiles_to_db()
            elif sys.argv[1] == "query":
                limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5
                query_profiles(limit)
            elif sys.argv[1] == "natural":
                question = sys.argv[2]
                execute_natural_query(question)

    except Exception as e:
        print(f"Error: {e}")