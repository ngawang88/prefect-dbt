import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

def db_reader(sql_query: Optional[str] = None, sql_file: Optional[str] = None):
    """
    Reads data from Redshift using either a SQL query string or a path to a .sql file.
    One of sql_query or sql_file must be provided.

    Parameters:
    - sql_query (str): The SQL query to execute.
    - sql_file (str): The path to a .sql file containing the query.

    Returns:
    - pd.DataFrame: The results of the query as a DataFrame.
    """
    if not (sql_query or sql_file):
        raise ValueError("Either sql_query or sql_file must be provided.")
    if sql_file:
        if not os.path.isfile(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        with open(sql_file, 'r') as f:
            sql_query = f.read()
    if not sql_query:
        raise ValueError("SQL query is empty after reading from file.")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT", "5439")
    db = os.getenv("REDSHIFT_DB")
    if not all([user, password, host, db]):
        raise ValueError("Missing one or more Redshift connection environment variables.")
    # Use the Redshift SQLAlchemy dialect
    conn_str = f"redshift+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        df = pd.read_sql(sql_query, conn)
    return df