from datetime import date
from sqlalchemy.orm import Session

from models import FileEventLog

def create_file_event_log_for_uploaded(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_file_event_log(
        session,
        file_name,
        collection_id,
        "UPLOADED",
        run_date
    )

def create_file_event_log_for_processing(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESSING",
        run_date
    )


def create_file_event_log_for_success(session: Session, file_name: str, collection_id: int, run_date: date):
    __create_file_event_log(
        session,
        file_name,
        collection_id,
        "PROCESS_SUCCESS",
        run_date
    )
    

def create_file_event_log_for_error(session: Session, file_name: str, collection_id: int, run_date: date, stage: str, error_message: str):
    __create_file_event_log(
        session,
        file_name,
        collection_id,
        f"{stage}_ERROR",
        run_date,
        error_message
    )


def __create_file_event_log(session: Session, file_name: str, collection_id: int, status: str, run_date: date, error_message: str = None):
    data = {
        'file_name': file_name,
        'collection_id': collection_id,
        'status': status,
        'run_date': run_date,
        'error_message': error_message
    } 
    
    FileEventLog.upsert(session, data)