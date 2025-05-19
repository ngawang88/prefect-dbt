import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import numpy as np
from datetime import datetime

from shared.loadCSV.loadCSV import load_csv_to_df

# Load environment variables from .env file
load_dotenv()

# Placeholder tasks for each subgraph/phase in import2RawRedcat

@task
def import_sales(raw_member):

    logger = get_run_logger()
    logger.info("Running sales subgraph..."+str(raw_member))
    return True

