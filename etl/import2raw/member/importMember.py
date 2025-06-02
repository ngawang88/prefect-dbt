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
def import_members(ctx):
    logger = get_run_logger()
    logger.info(ctx+":Running members subgraph...")


    # 1. Read all members CSV files from the Redcat directory
    redcat_dir = os.environ.get("REDCAT_DIR", "./rawFile/redcat")
    pattern = os.path.join(redcat_dir, "members_*.csv")
    files = glob.glob(pattern)
    logger.info(f"Found {len(files)} members files: {files}")
    if not files:
        logger.warning("No members files found. Skipping.")
        return None

    all_members = []
    for file in files:
        try:
            df = load_csv_to_df(file)
            #pd.read_csv(file, dtype=str, keep_default_na=False)
            logger.info(f"Read {len(df)} rows from {file}")
            # Header validation: check required columns
            required_cols = [
                'MemberNo', 'MemberID', 'Surname', 'GivenNames', 'UserName', 'ActiveStatus',
                'Registered', 'Verified', 'Referred', 'Email', 'CardType', 'FavouriteStoreName',
                'FavouriteStoreID', 'IssuingStoreName', 'IssuingStoreID', 'DateOfBirth',
                'RegistrationDateTime', 'RegistrationDate', 'RegistrationTime', 'VerificationDateTime',
                'VerificationDate', 'ExpiryDateTime', 'ExpiryDate', 'LastUpdateDate', 'LastUpdateDateTime',
                'CreationDate', 'Sex', 'SendEmail', 'SendSMS', 'Phone', 'Mobile', 'State', 'PostCode',
                'Address1', 'Address2', 'Suburb', 'Country', 'DeviceID', 'DeviceType', 'PackageName',
                'GroupName', 'GroupID', 'PointsAwarded', 'PointsRedeemed', 'MoneyAwarded',
                'MoneyRedeemed', 'PointsBalance', 'MoneyBalance', 'LastTxnDate', 'FirstTxnDate',
                'LastTxnDateTime', 'FirstTxnDateTime', 'LastAdminTxnDate', 'FirstAdminTxnDate',
                'LastAdminTxnDateTime', 'FirstAdminTxnDateTime', 'VerificationType'
            ]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"File {file} is missing columns: {missing_cols}")
                continue

            # Data type and format checks (example: date columns)
            date_cols = [
                'RegistrationDateTime', 'VerificationDateTime', 'ExpiryDateTime', 'LastUpdateDateTime',
                'CreationDate', 'LastTxnDate', 'FirstTxnDate', 'LastAdminTxnDate', 'FirstAdminTxnDate',
                'LastAdminTxnDateTime', 'FirstAdminTxnDateTime'
            ]
            for col in date_cols:
                if col in df.columns:
                    # Try to parse, fill errors with NaT
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Deduplicate by MemberID
            before = len(df)
            df = df.drop_duplicates(subset=['MemberID'])
            after = len(df)
            if before != after:
                logger.info(f"Deduplicated {before-after} duplicate MemberID rows in {file}")

            # Add source_name and source_row_count
            df['source_name'] = os.path.basename(file)
            df['source_row_count'] = len(df)

            all_members.append(df)
        except Exception as e:
            logger.error(f"Error processing {file}: {e}")

    if not all_members:
        logger.warning("No valid members data loaded.")
        return None

    members_df = pd.concat(all_members, ignore_index=True)
    logger.info(f"Total members loaded: {len(members_df)}")
    logger.info(f"Columns: {members_df.columns.tolist()}")
    #logger.info(members_df.head().to_string())

    # Save validated data as CSV in dbt seeds folder
    seeds_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../dbt/sch_tranform/seeds'))
    logger.info(f"Resolved seeds_dir: {seeds_dir}")
    os.makedirs(seeds_dir, exist_ok=True)
    seed_csv_path = os.path.join(seeds_dir, 'rc_rawmembers/rc_rawmembers_seed.csv')
    logger.info(f"Saving CSV to: {seed_csv_path}")
    members_df.to_csv(seed_csv_path, index=False)
    logger.info(f"Saved validated members data to {seed_csv_path}")

    return members_df
