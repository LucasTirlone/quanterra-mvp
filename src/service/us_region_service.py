import pandas as pd
import logging
from models import UsRegion

def update_regions(db_session, us_regions_csv):
    for idx, row in us_regions_csv.iterrows():
        us_region = get_us_region_object(row)
        
        logging.info(f"Update US Region for zip {us_region['zip']}")
        
        if pd.isna(us_region['zip']):
            continue
        
        UsRegion.upsert(db_session, us_region)


def get_us_region_by_zip(db_session, zip):
    if pd.isna(zip):
        return None
    return UsRegion.get_by_zip(db_session, zip)
 
    
def get_us_region_object(row):
    return {
        "zip" : row["PhysicalZip"],
        "region": row["CensusRegion"],
        "division": row["CensusDivision"]
    }
    