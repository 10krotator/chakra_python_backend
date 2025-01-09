from langchain_openai import OpenAI
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain_core.tools import Tool
from dotenv import load_dotenv
import os
import duckdb

load_dotenv()

# Get the token from environment variable
token = os.getenv("MOTHERDUCK_TOKEN")
if not token:
    raise ValueError("MOTHERDUCK_TOKEN environment variable is not set")

# Format the connection string correctly for MotherDuck shared database
conn_str = f'md:?motherduck_token={token}'
db = duckdb.connect(conn_str)

print(db.sql("SELECT * FROM linkedin_profiles.linkedin_full LIMIT 10").df())