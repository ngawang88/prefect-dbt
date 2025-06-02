from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
from typing import Optional, Literal

load_dotenv()

def db_writer(df: Optional[pd.DataFrame] = None, table_name: Optional[str] = None, schema:Optional[str] = None, sql_command: Optional[str] = None, sql_file: Optional[str] = None, if_exists: Literal['fail', 'replace', 'append'] = 'append'):
    """
    Writes data to Redshift. You can either:
    - Pass a DataFrame and table_name to write the DataFrame to the table.
    - Pass a SQL command string or a .sql file to execute arbitrary SQL (e.g., DDL or DML).
    """
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT", "5439")
    db = os.getenv("REDSHIFT_DB")
    if not all([user, password, host, db]):
        raise ValueError("Missing one or more Redshift connection environment variables.")
    conn_str = f"redshift+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(conn_str)
    if df is not None and table_name:
        # Write DataFrame to Redshift table
        df.to_sql(table_name, engine, index=False, if_exists=if_exists, schema=schema)
    elif sql_command or sql_file:
        if sql_file:
            if not os.path.isfile(sql_file):
                raise FileNotFoundError(f"SQL file not found: {sql_file}")
            with open(sql_file, 'r') as f:
                sql_command = f.read()
        if not sql_command:
            raise ValueError("SQL command is empty after reading from file.")
        with engine.connect() as connection:
            connection.execute(text(sql_command))
    else:
        raise ValueError("You must provide either a DataFrame and table_name, or a SQL command/file.")