from datetime import date
from sqlalchemy.orm import Session

from models import FileEventLog

def create_file_event_log_for_uploaded(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "UPLOADED",
        run_date
    )


def update_file_event_log_for_processing(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESSING",
        run_date
    )


def update_file_event_log_for_success(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESS_SUCCESS",
        run_date
    )
    

def update_file_event_log_for_error(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_or_update_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESS_ERROR",
        run_date
    )


def __create_or_update_file_event_log(session: Session, file_name: str, collection_id: int, status: str, run_date: date):
    data = {
        'file_name': file_name,
        'collection_id': collection_id,
        'status': status,
        'run_date': run_date
    } 
    
    FileEventLog.upsert(session, data)