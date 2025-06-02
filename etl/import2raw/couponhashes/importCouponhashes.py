import os
import glob
import pandas as pd
from prefect import task, get_run_logger
from shared.loadCSV.loadCSV import load_csv_to_df
from shared.dbUtils.src.shared.db_writer import db_writer

@task
def import_couponhashes(ctx=None):
    logger = get_run_logger()
    logger.info("Running couponhashes import task...")
    # Find the couponhashes CSV file(s)
    redcat_dir = os.environ.get("REDCAT_DIR", "./rawFile/redcat")
    pattern = os.path.join(redcat_dir, "couponhashes_*.csv")
    files = glob.glob(pattern)
    logger.info(f"Found {len(files)} couponhashes files: {files}")
    if not files:
        logger.warning("No couponhashes files found. Skipping.")
        return None
    all_coupons = []
    for file in files:
        try:
            df = load_csv_to_df(file)
            logger.info(f"Read {len(df)} rows from {file}")
            all_coupons.append(df)
        except Exception as e:
            logger.error(f"Error processing {file}: {e}")
    if not all_coupons:
        logger.warning("No valid couponhashes data loaded.")
        return None
    coupons_df = pd.concat(all_coupons, ignore_index=True)
    logger.info(f"Total couponhashes loaded: {len(coupons_df)}")
    # Truncate the table before loading new data
    try:
        db_writer(sql_command="TRUNCATE TABLE public.rc_rawcouponhashes;")
        logger.info("Truncated rc_rawcouponhashes table.")
    except Exception as e:
        logger.warning(f"Could not truncate rc_rawcouponhashes (may not exist yet): {e}")
    # Write to Redshift using db_writer
    try:
        db_writer(df=coupons_df, table_name="rc_rawcouponhashes", schema='public', if_exists="append")
        logger.info("Successfully wrote couponhashes data to Redshift table rc_rawcouponhashes.")
    except Exception as e:
        logger.error(f"Failed to write couponhashes data to Redshift: {e}")
        return None
    return coupons_df
