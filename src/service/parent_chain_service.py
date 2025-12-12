import logging
from typing import Any, Optional

import pandas as pd

from models import ParentChain

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
    Converts fields like 'ManualChange', 'ArchiveRecord' etc. to boolean.

    Accepts:
    - True/False
    - strings like 'VERDADEIRO', 'FALSO', 'TRUE', 'False', 'sim', 'não'
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


def _parse_chain_id(raw: Any) -> Optional[int]:
    """
    Converts ChainID to integer when possible.

    - If NaN/None -> None
    - If numeric string ('8', '42') -> int
    - If something like 'QR1001' -> None (we skip this line in MVP)
    """
    if pd.isna(raw):
        return None

    s = str(raw).strip()
    if not s:
        return None

    try:
        return int(s)
    except ValueError:
        # Example: 'QR1001', 'QR1010', etc.
        logger.warning(
            "Skipping ParentChains row with non-numeric ChainID: %r (only chains with numeric ID enter the MVP)",
            s,
        )
        return None


def upsert_parent_chains_from_excel(session, df_parent_chains: pd.DataFrame) -> None:
    """
    Reads the DataFrame corresponding to the spreadsheet
    'Table - Parent Chains 2025.09.06.xlsx' and upserts into parent_chains table.

    Rules:
    - Only inserts rows with numeric ChainID.
    - ParentChainId (which can be 'QR1001', etc.) is stored as text.
    - Converts NaN/NaT to None before sending to database.
    """
    for _, row in df_parent_chains.iterrows():
        raw_chain_id = row.get("ChainID")
        chain_name = row.get("ChainName")

        # Without a name, it doesn't make sense
        if pd.isna(chain_name):
            continue

        chain_id = _parse_chain_id(raw_chain_id)
        # If ChainID is not numeric (e.g., 'QR1001'), we skip this row
        if chain_id is None:
            continue

        data = {
            "chain_id": chain_id,
            "chain_name": str(chain_name),
            "chain_status": _clean_scalar(row.get("ChainStatus")),
            "url": _clean_scalar(row.get("URL")),

            # Conglomerate / parent info
            "parent_chain_id": _clean_scalar(row.get("ParentChainId")),
            "parent_chain_name": _clean_scalar(row.get("ParentChainName")),
            "stock_ticker": _clean_scalar(row.get("StockTicker")),

            # Curation / audit trail
            "manual_change": _clean_bool(row.get("ManualChange")) if "ManualChange" in row else False,
            "change_fields": _clean_scalar(row.get("ChangeFields")),
            "original_values": _clean_scalar(row.get("OriginalValues")),
            "change_reason": _clean_scalar(row.get("ChangeReason")),
            "modified_by": _clean_scalar(row.get("ModifiedBy")),
            "modified_date": _clean_scalar(row.get("ModifiedDate")),
            "archive_record": _clean_bool(row.get("ArchiveRecord")) if "ArchiveRecord" in row else False,
            "upload_timestamp": _clean_scalar(row.get("UploadTimestamp")),
        }

        ParentChain.upsert(session, data)
        logger.info("Upsert ParentChain chain_id=%s (%s)", chain_id, chain_name)