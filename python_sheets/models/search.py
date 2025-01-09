from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
# from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from operator import itemgetter
from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv
import duckdb
from sqlalchemy import create_engine, inspect
import duckdb_engine

class SQLQueryGenerator:
    def __init__(self, db_path: str = None):
        # Load environment variables
        load_dotenv()
        
        # Get API key and verify it exists
        self.api_key = os.getenv("NEW_OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        # Initialize OpenAI client
        self.llm = ChatOpenAI(
            api_key=self.api_key,
            model="gpt-3.5-turbo",
            temperature=0
        )
        
        # Initialize database connection with DuckDB
        token = os.getenv("MOTHERDUCK_TOKEN")

        # Create SQLAlchemy engine for DuckDB
        engine = create_engine(f'duckdb:///md:linkedin_profiles?motherduck_token={token}')

        # Create SQLDatabase instance using the SQLAlchemy engine
        self.db = SQLDatabase(engine=engine)
        
        # Set up SQL query generation components
        self.query_tool = QuerySQLDatabaseTool(db=self.db)
        self.write_query = create_sql_query_chain(self.llm, self.db)

        # Set up the prompt template for SQL generation
        sql_prompt = """You are a SQL query generator. Write only the SQL query with no additional text or formatting.
        Question: {input}
        Database schema ({top_k} tables):
        {table_info}"""
        
        self.sql_prompt = PromptTemplate.from_template(sql_prompt)
        
        # Create the chain that will output only the SQL query
        inspector = inspect(engine)
        columns = inspector.get_columns("linkedin_profiles")
        table_schema = "\n".join([f"- {col['name']}: {col['type']}" for col in columns])

        self.chain = (
            RunnablePassthrough.assign(
                top_k=lambda _: 1,
                table_info=lambda _: f"Table: linkedin_profiles\nColumns:\n{table_schema}"
            )
            | self.sql_prompt
            | self.llm
            | StrOutputParser()
        )

    def generate_query(self, question: str) -> str:
        """
        Generate a SQL query based on a natural language question
        
        Args:
            question (str): Natural language question about the database
            
        Returns:
            str: Generated SQL query
        """
        try:
            return self.chain.invoke({"input": question})
        except Exception as e:
            raise Exception(f"An unexpected error occurred: {str(e)}")
