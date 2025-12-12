import logging
from typing import Any, Optional

import pandas as pd

from models import Landlord

logger = logging.getLogger(__name__)


def _clean_scalar(value: Any) -> Optional[Any]:
    """
    Normalizes values from pandas DataFrame to something Postgres accepts.

    - If NaN/NaT -> None
    - Otherwise, returns the original value.
    """
    if pd.isna(value):
        return None
    return value


def _clean_bool(value: Any) -> bool:
    """
    Converts curation fields (ManualChange, ArchiveRecord etc.) to boolean.

    Accepts:
    - True/False
    - strings like 'TRUE', 'False', 'sim', 'não', '1'
    - integers (0/1)
    - NaN/None -> False
    """
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        return v in ("true", "verdadeiro", "1", "yes", "sim")
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _parse_landlord_id(raw: Any) -> Optional[str]:
    """
    Normalizes LandlordID from the spreadsheet:

    - If NaN/None -> None
    - Otherwise, converts to string and strips.
    """
    if pd.isna(raw):
        return None

    s = str(raw).strip()
    if not s:
        return None

    return s


def upsert_landlords_from_excel(session, df_landlords: pd.DataFrame) -> None:
    """
    Reads the DataFrame corresponding to the spreadsheet
    'Table - Landlords 2025.09.06.xlsx' and upserts into landlords table.

    Rules:
    - LandlordID is the primary key (text).
    - Converts NaN/NaT to None before sending to database.
    - Curation/boolean fields are standardized via _clean_bool.
    """
    if df_landlords is None or df_landlords.empty:
        logger.warning("upsert_landlords_from_excel: Empty DataFrame; nothing to do.")
        return

    for _, row in df_landlords.iterrows():
        raw_landlord_id = row.get("LandlordID")
        landlord_name = row.get("LandlordName")

        landlord_id = _parse_landlord_id(raw_landlord_id)

        # Without ID or name, we skip the row
        if not landlord_id or pd.isna(landlord_name):
            continue

        data = {
            "landlord_id": landlord_id,
            "landlord_name": str(landlord_name),

            # General landlord information
            "landlord_status": _clean_scalar(row.get("LandlordStatus")),
            "url": _clean_scalar(row.get("URL")),
            "sic_code": _clean_scalar(row.get("SICCode")),
            "naics_code": _clean_scalar(row.get("NAICSCode")),
            "primary_category": _clean_scalar(row.get("PrimaryCategory")),
            "categories": _clean_scalar(row.get("Categories")),
            "countries": _clean_scalar(row.get("Countries")),
            "property_count": _clean_scalar(row.get("PropertyCount")),

            # Market / public capital
            "is_public": _clean_bool(row.get("IsPublic")) if "IsPublic" in row else False,
            "stock_ticker": _clean_scalar(row.get("StockTicker")),
            "property_sector": _clean_scalar(row.get("PropertySector")),
            "property_subsector": _clean_scalar(row.get("PropertySubsector")),
            "index_name": _clean_scalar(row.get("IndexName")),
            "region_coverage": _clean_scalar(row.get("RegionCoverage")),
            "property_url": _clean_scalar(row.get("PropertyURL")),

            # Curation / audit trail
            "archive_record": _clean_bool(row.get("ArchiveRecord"))
            if "ArchiveRecord" in row
            else False,
            "manual_change": _clean_bool(row.get("ManualChange"))
            if "ManualChange" in row
            else False,
            "change_fields": _clean_scalar(row.get("ChangeFields")),
            "original_values": _clean_scalar(row.get("OriginalValues")),
            "change_reason": _clean_scalar(row.get("ChangeReason")),
            "modified_by": _clean_scalar(row.get("ModifiedBy")),
            "modified_date": _clean_scalar(row.get("ModifiedDate")),
            "upload_timestamp": _clean_scalar(row.get("UploadTimestamp")),
        }

        Landlord.upsert(session, data)
        logger.info("Upsert Landlord landlord_id=%s (%s)", landlord_id, landlord_name)
