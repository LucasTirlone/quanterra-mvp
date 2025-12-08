from datetime import date
from sqlalchemy.orm import Session

from src.models import FileEventLog

def create_file_event_log_for_processing(session: Session, file_name: str, collection_id: int, scrape_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESSING",
        scrape_date
    )


def update_file_event_log_for_success(session: Session, file_name: str, collection_id: int, scrape_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESS_SUCCESS",
        scrape_date
    )
    

def update_file_event_log_for_error(session: Session, file_name: str, collection_id: int, scrape_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESS_ERROR",
        scrape_date
    )


def __create_or_update_file_event_log(session: Session, file_name: str, collection_id: int, status: str, scrape_date: date):
    data = {
        'file_name': file_name,
        'collection_id': collection_id,
        'status': status,
        'scrape_date': scrape_date
    } 
    
    FileEventLog.upsert(session, data)