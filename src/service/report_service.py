import pandas as pd
import logging

from utils.cell_util import midpoint, full_address, parse_dateflex
from service.chain_scrapper_service import upsert_chain_scraper
from service.location_service import get_or_create_location, get_location_by_synthetic_location_id, get_status
from service.location_event_service import get_last_event, create_current_event, get_all_by_date_range
from service.us_region_service import get_us_region_by_zip

logging.basicConfig(level=logging.INFO)


def generate_report_for_chain_scraper(db_session, collection_id, df_chain_scraper_csv, df_collection_csv):
    logging.info(f"Generate chain_scraper report for collection_id {collection_id}")
    df_chain_scraper_sorted = df_chain_scraper_csv.sort_values(by=['ChainId', 'Date', 'Time'])

    outputs_chain_scraper = []
    
    run_check_count = 0
    last_chain_id = None
    for idx, row in df_chain_scraper_sorted.iterrows():
        chain_id = row['ChainId']
        date = row['Date']
        
        filtered_collection = df_collection_csv[
            (df_collection_csv['ChainId'] == chain_id) & 
            (df_collection_csv['LastUpdate'] == date)
        ].sort_values(by=["LastUpdate", "Status"])

        if chain_id != last_chain_id:
            logging.info(f"Generate data for ChainId {chain_id}")
            last_chain_id = chain_id
            run_check_count = 0
        elif verify_if_next_date_is_same(df_chain_scraper_sorted, idx):
            continue
        
        old_run_check_count = run_check_count
        chain_scraper, run_check_count = create_chain_scraper(db_session, collection_id, row, filtered_collection, run_check_count)
        
        if chain_scraper["run_check_count"] > old_run_check_count:
            row['ActualRunCheck'] = get_actual_run_check_cell_value(chain_scraper)
            row['DiffRunCheck'] = get_diff_run_check_cell_value(chain_scraper)
            row['RunCheckStatus'] = get_run_check_status_cell_value(chain_scraper)
            
        outputs_chain_scraper.append(row)
                
    return create_output_csv_file(outputs_chain_scraper, f"CollectionId-{collection_id}-ChainScraper.csv")


def generate_report_for_collection(db_session, collection_id, start_scraper_date, end_scraper_date, df_chain_scraper_csv, df_collection_csv):
    logging.info(f"Generate report for collection_id {collection_id} between {start_scraper_date} and {end_scraper_date}")
    df_collection_csv = df_collection_csv.sort_values(by=['ChainId', 'LastUpdate', 'Status', 'Longitude', 'Latitude'])
    
    midpoint_date = midpoint(start_scraper_date, end_scraper_date)

    outputs_collection = {}
    last_chain_id = None

    error_count = 0
    chain_id_count = 0
    for c_idx, c_row in df_collection_csv.iterrows():
        try:
            chain_id = c_row["ChainId"]
            last_update = parse_dateflex(c_row["LastUpdate"])
            
            if chain_id != last_chain_id:
                logging.info(f"Generate data for ChainId {chain_id}")
                last_chain_id = chain_id
                if error_count > 0:
                    logging.info(f"Rows with error: {error_count} from {chain_id_count} lines for ChainId {chain_id} and ScrapeDate {last_update}")
                    error_count = 0
                
                chain_id_count = 0
                
            location, suspected_hash_change = get_or_create_location(db_session, c_row, midpoint_date)
            last_event = get_last_event(db_session, c_row, location)
            location.update_last_event_date(db_session, midpoint_date)
            
            current_event = create_current_event(db_session, c_row, location, last_event, suspected_hash_change, midpoint_date)    
            chain_id_count += 1
                        
            us_region = get_us_region_by_zip(db_session, location.zip)
            output = get_output(location, current_event, last_event, us_region)
            outputs_collection[generate_output_key(current_event)] = output
        except Exception as e:
            error_count += 1
        
    return create_output_csv_file(outputs_collection.values(), f"CollectionId-{collection_id}-enriched.csv")


def generate_output_key(event):
    return f"{event.synthetic_location_id}:{event.scrape_date}:{event.event_type}"


def direct_report(db_session, start_scraper_date, end_scraper_date):
    logging.info(f"Generate direct report between {start_scraper_date} and {end_scraper_date}")
    location_events = get_all_by_date_range(db_session, start_scraper_date, end_scraper_date)
    
    outputs = []
    map_location = {}
    for location_event in location_events:
        location = get_location_by_synthetic_location_id(db_session, location_event.synthetic_location_id)
        last_event = location_event.last_location_event
        us_region = get_us_region_by_zip(db_session, location.zip)
        outputs.append(get_output(location, location_event, last_event, us_region))
    
    return create_output_csv_file(outputs, f"Direct-Report-{start_scraper_date}-{end_scraper_date}.csv")


def verify_if_next_date_is_same(df_chain_scraper, index):
    last_position = len(df_chain_scraper) - 1
    
    if index == last_position:
        return False
    
    current_chain_scraper = df_chain_scraper.iloc[index]
    next_chain_scraper = df_chain_scraper.iloc[index +1]
    return next_chain_scraper["Date"] == current_chain_scraper["Date"]


def get_output(location, current_event, last_event, us_region): 
    return {
        "ChainName": location.chain_name,
        "Address": full_address(location.address_normalized, location.address_complement, location.store_number),
        "City": location.city,
        "State": location.state,
        "Zip":  location.zip,
        "UsRegion": us_region.region if us_region else "", 
        "UsDivision": us_region.division if us_region else "",
        "Status": get_status(current_event.event_type),
        "RemodelType": current_event.remodel_type if hasattr(current_event, "remodel_type") else "N/A",
        "OpenAtEstimated": current_event.current_opened_at_estimated,
        "ClosedAtEstimated": current_event.current_closed_at_estimated,
        "ExplainWindow": get_explain_window(current_event, last_event),
        "SuspectedHashChange": current_event.suspected_hash_change
    }


def get_explain_window(current_event, last_event):
    if last_event is None:
        return "First scrape " + str(current_event.scrape_date)
    return "Scrape " + str(last_event.scrape_date) + " -> " + str(current_event.scrape_date)


def create_chain_scraper(db_session, collection_id, row, filtered_collection, run_check_count):
    added_count = len(filtered_collection[filtered_collection['Status'] == "Added"])
    removed_count = len(filtered_collection) - added_count

    run_check_count += added_count
    chain_scraper = upsert_chain_scraper(db_session, collection_id, row, run_check_count)
    
    run_check_count -= removed_count
    return chain_scraper, run_check_count


def get_actual_run_check_cell_value(chain_scraper):
    if chain_scraper["run_check_count"] == 0:
        return str("N/A")
    return chain_scraper["run_check_count"]


def get_diff_run_check_cell_value(chain_scraper):
    if chain_scraper["run_check_count"] == 0:
        return str("N/A")
    return chain_scraper["us_location_count"] - chain_scraper["run_check_count"]


def get_run_check_status_cell_value(chain_scraper):
    if chain_scraper["run_check_count"] == 0:
        return str("N/A")
    return "MATCHED" if chain_scraper["us_location_count"] == chain_scraper["run_check_count"]  else "UNMATCHED"


def create_output_csv_file(outputs, csv_name):
    try:
        df = pd.DataFrame(outputs)
        df.to_csv(csv_name, index=False, encoding='utf-8')
        logging.info(f"Arquivo CSV '{csv_name}' criado com sucesso.")
    except Exception as e:
        raise RuntimeError(f"Error generating export csv - Erro: {e}")
