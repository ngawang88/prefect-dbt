
import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import numpy as np
from datetime import datetime


@task
def log_etl_ingestion(df, source_name):
    logger = get_run_logger()
    logger.info(f"Ingestion log for {source_name}: {len(df)} records ingested.")
    # You can expand this to log to a file or DB if needed

@task
def log_etl_rejection(file, error):
    logger = get_run_logger()
    logger.error(f"Rejection log for {file}: {error}")
    # You can expand this to log to a file or DB if needed

@task
def header_validator(df, required_cols, file):
    logger = get_run_logger()
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"File {file} is missing columns: {missing_cols}")
        raise ValueError(f"Missing columns: {missing_cols}")
    logger.info(f"Header validation passed for {file}")
    return True

@task
def save_errors(errors, error_file):
    logger = get_run_logger()
    if errors:
        with open(error_file, 'a') as f:
            for err in errors:
                f.write(f"{err}\n")
        logger.error(f"Saved errors to {error_file}")
    else:
        logger.info("No errors to save.")

@task
def dedup_by_column(df, column):
    logger = get_run_logger()
    before = len(df)
    df = df.drop_duplicates(subset=[column])
    after = len(df)
    if before != after:
        logger.info(f"Deduplicated {before-after} duplicate rows by {column}")
    return df

@task
def add_source_name_and_count(df, file):
    df['source_name'] = os.path.basename(file)
    df['source_row_count'] = len(df)
    return df

@task
def convert_to_rc_raw_member(df):
    logger = get_run_logger()
    # Example conversion logic based on CloverDX graph (add more as needed)
    # Convert MemberID to int, handle dates, etc.
    if 'MemberID' in df.columns:
        df['MemberID'] = pd.to_numeric(df['MemberID'], errors='coerce')
    # Example: convert date columns to string in the required format
    date_cols = [
        'RegistrationDateTime', 'VerificationDateTime', 'ExpiryDateTime', 'LastUpdateDateTime',
        'CreationDate', 'LastTxnDate', 'FirstTxnDate', 'LastAdminTxnDate', 'FirstAdminTxnDate',
        'LastAdminTxnDateTime', 'FirstAdminTxnDateTime'
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
    logger.info("Converted DataFrame for rc_rawMembers.")
    return df