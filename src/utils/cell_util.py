
import re
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, Any

ABBR_MAP = {
    " STREET ": " ST ",
    " AVENUE ": " AVE ",
    " ROAD ": " RD ",
    " DRIVE ": " DR ",
    " BOULEVARD ": " BLVD ",
    " PLACE ": " PL ",
    " SQUARE ": " SQ ",
    " LANE ": " LN ",
    " TERRACE ": " TER ",
    " COURT ": " CT ",
    " HIGHWAY ": " HWY ",
}


def slugify_string(string: str) -> str:
    s = (string or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return re.sub(r"-+", "-", s).strip("-")


def normalize_address(addr: str) -> str:
    if not addr or pd.isna(addr):
        return ""
    s = f" {addr.upper()} "
    for k, v in ABBR_MAP.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def title_case_city(string: str) -> str:
    return (string or "").title().strip()


def parse_dateflex(x: Any) -> Optional[date]:
    if pd.isna(x):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None
    

def midpoint(a: Optional[date], b: Optional[date]) -> Optional[date]:
    if not a or not b:
        return b or a
    delta = (b - a).days
    return a + timedelta(days=delta // 2)


def full_address(address, complement, store_number):
    return address + ( (", " + complement) if complement else "") + ( (", " + store_number) if store_number else "")