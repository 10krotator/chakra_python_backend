from python_sheets.models.search import SQLQueryGenerator

# Initialize the query generator
query_gen = SQLQueryGenerator()

# Print the available tables and schema (optional but helpful)
query_gen.print_query_tool_info()

# Ask your question
question = "Show me all columns from the linkedin_profiles table limit 5"
sql_query = query_gen.generate_query(question)

print("\nGenerated SQL Query:")
print(sql_query)