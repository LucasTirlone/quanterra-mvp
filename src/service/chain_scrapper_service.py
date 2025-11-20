
from models import ChainScrape
from utils.data_util import parse_time_string


def upsert_chain_scraper(session, collection_id, row, run_check_count):  
    chain_scrapper = {  
        "chain_id": row['ChainId'],     
        "chain_name": row['ChainName'],  
        "collection_id": collection_id,
        "scrape_date": row['Date'],  
        "scrape_time": parse_time_string(row['Time']), 
        "us_location_count": row['UsLocationCount'],
        "location_count": row['LocationCount'],
        "run_check_count": run_check_count if run_check_count else 0,
    }
    
    ChainScrape.upsert(session, chain_scrapper)
    return chain_scrapper


def get_all_chain_scrape(session, collection_id, start_date, end_date):
    return ChainScrape.get_all_by_collection_id(session, collection_id, start_date, end_date)
        