import pandas as pd
import hashlib
from utils.cell_util import slugify_string
from models import Location
from utils.cell_util import slugify_string, normalize_address,title_case_city, parse_dateflex
from utils.data_util import get_zip_treated, truncate, clean_dict_for_sqlalchemy

from datetime import datetime, timedelta


def update_location_status(session, update_location_status_csv):
    for idx, row in update_location_status_csv.iterrows():
        chain_id = row["ChainId"]
        status = row["Status"]
        
        if pd.isna(chain_id) or pd.isna(status):
            continue
        
        Location.update_status_by_chain_id(session, chain_id, status)
        

def close_location_by_limit(session, weeks):
    limit_date = datetime.now().date() - timedelta(weeks=weeks)
    Location.close_when_limit_expires(session, limit_date)
        

def get_or_create_location(session, collection_row, midpoint):
    suspected_hash_change = False
    
    hashId = collection_row['HashId']
    location = get_location_by_partner_hash_id(session, hashId)
    if location is not None:
        location.update_status(session, get_status(collection_row['Status']), midpoint)
        return location, suspected_hash_change
    
    synthetic_id = get_synthetic_location_id(collection_row)
    location = get_location_by_synthetic_location_id(session, synthetic_id)
    if location is not None:
        location.update_status(session, get_status(collection_row['Status']), midpoint)
        suspected_hash_change = True
        return location, suspected_hash_change

    return create_location(session, collection_row, synthetic_id, midpoint), suspected_hash_change


def get_location_by_partner_hash_id(session, partner_hash_id):
    location = session.query(Location).filter_by(partner_hash_id=partner_hash_id).first()
    return location


def get_synthetic_location_id(collection_row):
    lat = collection_row['Latitude']
    lon = collection_row['Longitude']
    
    if pd.isna(lat) or pd.isna(lon):
        raise ValueError("Latitude OR Longitude are blank, cannot generate synthetic_location_id")
    
    lat = truncate(float(lat), 4)
    lon = truncate(float(lon), 4)
    
    hash_input = f"{lat}:{lon}"

    store_number = collection_row["StoreNumber"]    
    if pd.isna(store_number):
        hash_input = hash_input + f":{store_number}" 
    
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def get_location_by_synthetic_location_id(session, synthetic_location_id):
    location = session.query(Location).filter_by(synthetic_location_id=synthetic_location_id).first()
    return location


def get_location_treated(collection_row, synthetic_location_id, midpoint):
    chain_name = collection_row["ChainName"]
    chain_slug = slugify_string(chain_name)
    address = normalize_address(collection_row["Address"])
    city = title_case_city(collection_row["City"])
    state = collection_row["State"].upper().strip()
    zip_code = get_zip_treated(collection_row["PostalCode"])
    
    return {
        'synthetic_location_id': synthetic_location_id,
        'chain_id': collection_row['ChainId'],
        'chain_name': chain_name,
        'store_name': collection_row.get("StoreName", ""),
        'chain_slug': chain_slug,
        'partner_hash_id': collection_row['HashId'],
        'address_normalized': address,
        'address_complement': collection_row.get('Address2', ""),
        'store_number': collection_row.get('StoreNumber', ""),
        'phone_number': collection_row.get('PhoneNumber', ""),
        'parent_chain_id': collection_row['ParentChainId'],
        'parent_chain_name': collection_row['ParentChainName'],
        'coming_soon': collection_row.get('ComingSoon', False),
        'store_hours': collection_row.get('StoreHours', ""),
        'status': get_status(collection_row['Status']), 
        'latitude': collection_row['Latitude'],
        'longitude': collection_row['Longitude'],
        'site_id': collection_row.get('SiteId', ""),
        'city': city,
        'state': state,
        'zip': zip_code,
        'opened_at_estimated': midpoint
    }
    

def create_location(session, collection_row, synthetic_location_id, midpoint):
    location_data = get_location_treated(collection_row, synthetic_location_id, midpoint)
    location_data = clean_dict_for_sqlalchemy(location_data)
    Location.upsert(session, location_data)
    return Location(**location_data)


def get_status(status_str):
    status_str = (status_str or "").strip().lower()
    if status_str == "added":
        return 'OPEN'
    elif status_str == "removed":
        return 'CLOSE'
    raise ValueError(f"Unknown status: {status_str}")
