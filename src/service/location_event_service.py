
from models import LocationEvent
from utils.cell_util import parse_dateflex
from utils.data_util import clean_dict_for_sqlalchemy


def get_last_event(session, collection_row, location):
    return (
        session.query(LocationEvent)
        .filter_by(synthetic_location_id=location.synthetic_location_id)
        .filter(LocationEvent.scrape_date < parse_dateflex(collection_row["LastUpdate"]))
        .order_by(LocationEvent.event_date_estimated.desc())
        .first()
    )


def create_current_event(session, collection_row, location, last_event, suspected_hash_change, midpoint_date):
    event  = get_basic_location_event_data(collection_row, location, suspected_hash_change)
    event['event_date_estimated'] = midpoint_date
    event['suspected_hash_change'] = False
    
    if last_event:
        event['last_location_event_id'] = last_event.id
        if event['event_type'] == 'Added' and last_event.event_type == 'Removed':
            remodel_type = 'SHORT' if has_short_remodel(last_event, event) else 'LONG'
            event['remodel_type'] = remodel_type
            last_event.update_remodel(session, remodel_type)
    
    event = clean_dict_for_sqlalchemy(event)
    LocationEvent.upsert(session, event)
    return LocationEvent(**event)


def get_all_by_date_range(db_session, start_date, end_date):
    return LocationEvent.get_all_by_date_range(db_session, start_date, end_date)


def get_basic_location_event_data(collection_row, location, suspected_hash_change):
    return {
        'synthetic_location_id': location.synthetic_location_id,
        'chain_id': location.chain_id,
        'event_type': collection_row['Status'],
        'suspected_hash_change': suspected_hash_change,
        'scrape_date': parse_dateflex(collection_row['LastUpdate']),
        'current_opened_at_estimated': location.opened_at_estimated,
        'current_closed_at_estimated': location.closed_at_estimated
    }
    

def has_short_remodel(closed_event, reopen_event):
    delta = (reopen_event['event_date_estimated'] - closed_event.event_date_estimated).days
    return (delta is not None and delta < 365)