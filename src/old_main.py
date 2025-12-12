import csv
import logging
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv

from service.db_service import get_db_session
from service.report_service import generate_report_for_collection, generate_report_for_chain_scraper, direct_report
from service.us_region_service import update_regions
from service.location_service import update_location_status, close_location_by_limit
from service.quality_report_service import generate_quality_report_and_save
from partner.chain_scrapes_api import generate_chain_scrape_in_intervals
from partner.collection_api import generate_reports_in_intervals
from service.center_service import update_centers_from_excel
from service.landlord_service import upsert_landlords_from_excel
from service.parent_chain_service import upsert_parent_chains_from_excel

logging.basicConfig(level=logging.INFO)

df_us_region_csv_path = './examples/Table - US Regions 2025.09.csv'
chain_scraper_csv_path = './examples/chainscrapes_collection_303288.csv'
collection_csv_path = './examples/US Pet_Stores_Collection 2025.05.20.csv'
open_close_chain_csv = './examples/OpenClose.csv'


df_chain_scraper_csv = pd.read_csv(chain_scraper_csv_path)
df_collection_csv = pd.read_csv(collection_csv_path, low_memory=False)
df_open_close_chain_csv = pd.read_csv(open_close_chain_csv)

collection_id = 303288

def main():
    logging.info("Start Project.")
    load_dotenv()
    session = get_db_session()
    
    now = datetime.now().date()
    start_date = now - timedelta(weeks=1)
    end_date = now

    df_us_region_csv = pd.read_excel(f"./examples/Table - US Regions 2025.09.06.xlsx")
    #update_regions(session, df_us_region_csv)

    df_landlords_csv = pd.read_excel(f"./examples/Table - Landlords 2025.09.06.xlsx")
    #upsert_landlords_from_excel(session, df_landlords_csv)
    
    df_parent_chains_csv = pd.read_excel(f"./examples/Table - Parent Chains 2025.09.06.xlsx")
    #upsert_parent_chains_from_excel(session, df_parent_chains_csv)
    
    df_centers_csv = pd.read_excel(f"./examples/Table - Centers 2025.10.08.xlsx")
    #update_centers_from_excel(session, df_centers_csv)


# For now, it won't work because we need to be consuming directly from the API.
def generate_report_for_backfill(session, weeks=16):
    start_date = datetime.now().date() - timedelta(weeks=weeks)
    end_date = start_date + timedelta(weeks=1)
    
    for i in range(weeks):
        generate_report(session, start_date, end_date)
        start_date = end_date
        end_date = start_date + timedelta(weeks=1)


def generate_report(session, start_date, end_date):
    generate_quality_report_and_save(df_collection_csv, collection_id, f"CollectionId-{collection_id}-quality-report.csv", end_date)
    generate_report_for_chain_scraper(session, collection_id, df_chain_scraper_csv, df_collection_csv)
    generate_report_for_collection(session, collection_id, start_date, end_date, df_collection_csv)
    
    
def generate_direct_report_for_month(session):
    now = datetime.now().date()
    start_date = now - timedelta(weeks=4)
    end_date = now
    generate_report(session, start_date, end_date)
    

def generate_direct_report_for_quarter(session):
    now = datetime.now().date()
    start_date = now - timedelta(weeks=12)
    end_date = now
    generate_report(session, start_date, end_date)
    

def generate_direct_report_for_quarter(session):
    now = datetime.now().date()
    start_date = now - timedelta(weeks=26)
    end_date = now
    generate_report(session, start_date, end_date)
    

def generate_direct_report_for_quarter(session):
    now = datetime.now().date()
    start_date = now - timedelta(weeks=52)
    end_date = now
    generate_report(session, start_date, end_date)
    

if __name__ == "__main__":
    main()
