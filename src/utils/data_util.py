
import re
import numpy as np
import pandas as pd
from typing import Any, Dict, Optional

from datetime import datetime, time
from pytz import timezone


def format_timezone(date):
    if date.tzinfo is None:
        return date.replace(tzinfo=timezone.utc)
    return date


def format_as_date(dt):
    return dt.strftime("%Y-%m-%d")
    

def parse_time_string(time_string: str) -> Optional[time]:
    """
    Parses a time string in the format "HH:MM:SS" or "HH:MM:SS.mmm...", 
    handling both dot and comma as decimal separators.
    """
    
    time_pattern = r'(\d{2}:\d{2}:\d{2}(?:[\.,]\d+)?)\s*'
    match = re.search(time_pattern, time_string)
    
    if match:
        raw_time_part = match.group(1)
        cleaned_time_str = raw_time_part.replace(',', '.')
    
        if '.' in cleaned_time_str:
            time_format = '%H:%M:%S.%f'
        else:
            time_format = '%H:%M:%S'
        
        try:
            dt_object = datetime.strptime(cleaned_time_str, time_format)
            return dt_object.time()
        
        except ValueError as e:
            raise ValueError(
                f"Error formatting string '{time_string}' for Time class. Cleaned string: '{cleaned_time_str}'. Error: {e}"
            ) from e
    
    raise ValueError(f"Error formatting string '{time_string}' for Time class: No Match found for HH:MM:SS.")


def get_zip_treated(zip_str):
    if pd.isna(zip_str):
        return None
    
    match = re.search(r"(\d{5})", zip_str)
    return match.group(1) if match else None


def truncate(number, decimals):
    factor = 10 ** decimals
    return int(number * factor) / factor


def clean_dict_for_sqlalchemy(data: Dict[str, Any]) -> Dict[str, Any]:
    cleaned_data = {}
    for key, value in data.items():
        if isinstance(value, float) and np.isnan(value):
            cleaned_data[key] = None
        else:
            cleaned_data[key] = value
            
    return cleaned_data