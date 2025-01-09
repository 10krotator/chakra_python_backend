from chakra_py import Chakra
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv
from typing import Optional, Union, List
import logging

class ChakraToMotherDuckLoader:
    def __init__(self, motherduck_db: str = "my_db"):
        """
        Initialize loader for transferring data from Chakra to MotherDuck
        
        Args:
            motherduck_db (str): Name of the MotherDuck database
        """
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Load environment variables
        load_dotenv()
        
        # Initialize connections
        self._init_chakra()
        self._init_motherduck(motherduck_db)

    def _init_chakra(self):
        """Initialize Chakra client"""
        try:
            session_key = os.getenv('CHAKRA_DB_SESSION_KEY')
            if not session_key:
                raise ValueError("CHAKRA_DB_SESSION_KEY not found in environment variables")

            self.chakra_client = Chakra(session_key)
            self.chakra_client.login()
            self.logger.info("Successfully connected to Chakra")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chakra client: {str(e)}")
            raise

    def _init_motherduck(self, db_name: str):
        """Initialize MotherDuck connection"""
        try:
            self.motherduck_conn = duckdb.connect(f'md:{db_name}')
            self.logger.info(f"Successfully connected to MotherDuck database: {db_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect to MotherDuck: {str(e)}")
            raise

    def load_to_motherduck(
        self,
        chakra_query: str,
        table_name: str,
        batch_size: Optional[int] = None,
        replace: bool = True,
        clean_columns: bool = True
    ) -> None:
        """
        Load data from Chakra to MotherDuck
        
        Args:
            chakra_query (str): SQL query to execute against Chakra
            table_name (str): Name of the table to create in MotherDuck
            batch_size (int, optional): Size of batches for loading large datasets
            replace (bool): If True, replace existing table; if False, append
            clean_columns (bool): If True, clean column names for compatibility
        """
        try:
            self.logger.info(f"Executing Chakra query: {chakra_query}")
            
            if batch_size:
                self._batch_load(chakra_query, table_name, batch_size, replace, clean_columns)
            else:
                self._single_load(chakra_query, table_name, replace, clean_columns)
                
        except Exception as e:
            self.logger.error(f"Failed to transfer data: {str(e)}")
            raise

    def _single_load(
        self,
        query: str,
        table_name: str,
        replace: bool,
        clean_columns: bool
    ) -> None:
        """Handle single batch data load"""
        # Execute Chakra query
        df = self.chakra_client.execute(query)
        
        if clean_columns:
            df.columns = df.columns.str.replace(' ', '_').str.replace('[^0-9a-zA-Z_]', '')
        
        # Load to MotherDuck
        self._load_dataframe(df, table_name, replace)
        
        self.logger.info(f"Loaded {len(df)} rows into {table_name}")

    def _batch_load(
        self,
        query: str,
        table_name: str,
        batch_size: int,
        replace: bool,
        clean_columns: bool
    ) -> None:
        """Handle batched data load"""
        offset = 0
        first_batch = True
        
        while True:
            # Add LIMIT and OFFSET to query
            batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
            
            # Get batch from Chakra
            df = self.chakra_client.execute(batch_query)
            
            if df.empty:
                break
                
            if clean_columns:
                df.columns = df.columns.str.replace(' ', '_').str.replace('[^0-9a-zA-Z_]', '')
            
            # Load to MotherDuck (replace only on first batch if needed)
            self._load_dataframe(df, table_name, replace and first_batch)
            
            self.logger.info(f"Loaded batch of {len(df)} rows (offset: {offset})")
            
            offset += batch_size
            first_batch = False
            
            if len(df) < batch_size:
                break

    def _load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        replace: bool
    ) -> None:
        """Load DataFrame to MotherDuck"""
        self.motherduck_conn.register('temp_df', df)
        
        if replace:
            self.motherduck_conn.execute(f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name} AS SELECT * FROM temp_df;
            """)
        else:
            self.motherduck_conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df WHERE 1=0;
                INSERT INTO {table_name} SELECT * FROM temp_df;
            """)

    def query_motherduck(self, query: str) -> pd.DataFrame:
        """Execute a query against MotherDuck"""
        try:
            return self.motherduck_conn.execute(query).df()
        except Exception as e:
            self.logger.error(f"Failed to query MotherDuck: {str(e)}")
            raise

    def close(self):
        """Close all connections"""
        try:
            self.motherduck_conn.close()
            self.logger.info("Closed all connections")
        except Exception as e:
            self.logger.error(f"Error closing connections: {str(e)}")


# Example usage
if __name__ == "__main__":
    # Initialize loader
    loader = ChakraToMotherDuckLoader(motherduck_db="linkedin_profiles")

    try:
        # # Example 1: Simple load
        # loader.load_to_motherduck(
        #     chakra_query="SELECT * FROM linkedin_profiles LIMIT 1000",
        #     table_name="linkedin_sample"
        # )

        # Example 2: Batch load
        loader.load_to_motherduck(
            chakra_query="SELECT * FROM linkedin_profiles",
            table_name="linkedin_full",
            batch_size=1000,
            replace=True
        )

        # Example 3: Query the loaded data
        result = loader.query_motherduck("""
            SELECT COUNT(*) as total_profiles 
            FROM linkedin_full
        """)
        print(f"Total profiles loaded: {result['total_profiles'][0]}")

    finally:
        # Clean up
        loader.close()