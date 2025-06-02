import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import numpy as np
from datetime import datetime

from shared.loadCSV.loadCSV import load_csv_to_df
from shared.dbUtils.src.shared.db_reader import db_reader

# Load environment variables from .env file
load_dotenv()

# Placeholder tasks for each subgraph/phase in import2RawRedcat

@task
def import_sales(raw_member):
    logger = get_run_logger()
    logger.info("Running sales subgraph..." + str(raw_member))
    # Read from Redshift database
    #sql_query = "SELECT * FROM public.rc_rawmembers"
    try:
        df = db_reader( sql_file='etl/import2raw/sales/sql/select.sql')
        logger.info(f"Loaded {len(df)} rows from rc_rawcustomer")
    except Exception as e:
        logger.error(f"Failed to load data from Redshift: {e}")
        return None
    return df

