import pandas as pd
import sqlite3
from pathlib import Path

def parquet_to_sqlite(parquet_path: str, sqlite_path: str, table_name: str) -> None:
    """
    Convert a Parquet file to SQLite database.
    
    Args:
        parquet_path (str): Path to the input Parquet file
        sqlite_path (str): Path to the output SQLite database
        table_name (str): Name of the table to create in SQLite
    """
    try:
        # Read the Parquet file
        df = pd.read_parquet(parquet_path)
        
        # Create a SQLite connection
        conn = sqlite3.connect(sqlite_path)
        
        # Write the dataframe to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"Successfully converted {parquet_path} to SQLite database at {sqlite_path}")
        
    except Exception as e:
        print(f"Error converting Parquet to SQLite: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Example usage
    # Single file conversion
    parquet_to_sqlite("python_sheets/data/train-00000-of-00002.parquet", "python_sheets/data/linkedin_sqlite.db", "linkedin_profiles")
    
    # Batch conversion
    # batch_convert_parquet_files("path/to/parquet/files", "output.db")
    pass
