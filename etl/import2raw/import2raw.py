import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import numpy as np
from datetime import datetime

#from shared.loadCSV.loadCSV import load_csv_to_df
from etl.import2raw.couponhashes.importCouponhashes import import_couponhashes
from etl.import2raw.locations.importLocations import import_locations
from etl.import2raw.member.importMember import import_members
from etl.import2raw.sales.importSales import import_sales
# Load environment variables from .env file
load_dotenv()



@flow
def import2raw():
    # Phase 0: Members
    ctx = 'MainFull'
    #raw_member = import_members(ctx)
    # Phase 1: Sales
    raw_sale  = import_sales(ctx)

    # Phase 2: Coupon Hashes
    couponhashes = import_couponhashes(ctx)

    # Phase 3: Locations
    locations = import_locations(ctx)
    
    return True