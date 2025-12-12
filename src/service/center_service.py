#
# Responsible for reading the spreadsheet
#   "Table - Centers 2025.10.08.xlsx"
# and populating:
#   - centers table
#   - center_landlords table (n:n relationship Center ↔ Landlord)
#
# Main rules (from product documentation):
# - One record in centers per SiteId.
# - In center_landlords, up to two landlords per center:
#       (SiteId, LandlordID, Ownership%)
#       (SiteId, LandlordID2, CoOwnership%)
# - Whenever we reload the spreadsheet, we clean the old links
#   for that SiteId in center_landlords and recreate the new snapshot. 

import logging
from typing import Any, Optional

import pandas as pd

from models import Center, CenterLandlord  # <- now we also use CenterLandlord

logger = logging.getLogger(__name__)


def _clean_scalar(value: Any) -> Optional[Any]:
    """
    Normalizes values from pandas DataFrame to something Postgres accepts.

    - If NaN/NaT -> None
    """
    if pd.isna(value):
        return None
    return value


def _clean_bool(value: Any) -> bool:
    """
    Converts fields like 'ArchiveRecord', 'ManualChange' to boolean.
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


def _clean_landlord_id(value: Any) -> Optional[int]:
    """
    Normalizes LandlordID / LandlordID2 to int or None.

    - NaN / empty -> None
    - "123"       -> 123
    - 123.0       -> 123
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError):
            logger.warning("Could not convert numeric landlord_id: %r", value)
            return None

    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        try:
            return int(v)
        except ValueError:
            logger.warning("Could not convert string landlord_id: %r", value)
            return None

    return None


def _clean_ownership_pct(value: Any) -> Optional[float]:
    """
    Normalizes Ownership% / CoOwnership% to float or None.

    Accepts formats:
    - NaN / empty -> None
    - 50          -> 50.0
    - 50.0        -> 50.0
    - "50"        -> 50.0
    - "50%"       -> 50.0
    - "50,5%"     -> 50.5
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None

        # Remove percentage symbol if present
        if v.endswith("%"):
            v = v[:-1].strip()

        # Replace comma with dot (for BR/EU format)
        v = v.replace(",", ".")

        try:
            return float(v)
        except ValueError:
            logger.warning("Could not convert Ownership%%: %r", value)
            return None

    return None


def update_centers_from_excel(session, df_centers: pd.DataFrame) -> None:
    """
    Reads the DataFrame corresponding to the spreadsheet
    'Table - Centers 2025.10.08.xlsx' and upserts into centers table
    + maintains links in center_landlords.

    Key rules (see product docs):
    - centers: one record per SiteId with title, address, metrics and audit trail.
    - center_landlords:
        * deletes old links for the SiteId
        * recreates up to two records (LandlordID / LandlordID2 + Ownership). 
    """

    for _, row in df_centers.iterrows():
        raw_site_id = row.get("SiteId")
        title = row.get("Title")

        # SiteId and Title are mandatory
        if pd.isna(raw_site_id) or pd.isna(title):
            continue

        site_id = str(raw_site_id).strip()
        if not site_id:
            continue

        # -------------------------
        # 1) Upsert in centers
        # -------------------------
        data = {
            # Basic identity
            "site_id": site_id,
            "title": str(title),

            # Tipo / formato
            "center_type": _clean_scalar(row.get("Type")),
            "format": _clean_scalar(row.get("Format")),

            # Location
            "address": _clean_scalar(row.get("Address")),
            "address2": _clean_scalar(row.get("Address2")),
            "city": _clean_scalar(row.get("City")),
            "region": _clean_scalar(row.get("Region")),
            "postal_code": _clean_scalar(row.get("PostalCode")),
            "country": _clean_scalar(row.get("Country")),

            # Geometry / metrics
            "latitude": _clean_scalar(row.get("Latitude")),
            "longitude": _clean_scalar(row.get("Longitude")),
            "gla": _clean_scalar(row.get("GLA")),
            "units": _clean_scalar(row.get("Units")),
            "year_opened": _clean_scalar(row.get("YearOpened")),
            "location_count": _clean_scalar(row.get("LocationCount")),
            "anchor_count": _clean_scalar(row.get("AnchorCount")),
            "anchor_chains": _clean_scalar(row.get("AnchorChains")),

            # Standardized version of country/state/postal code
            "country_std": _clean_scalar(row.get("CountryStd")),
            "state_std": _clean_scalar(row.get("StateStd")),
            "postal_code_std": _clean_scalar(row.get("PostalCodeStd")),

            # Audit / curation trail
            "archive_record": _clean_bool(row.get("ArchiveRecord")),
            "manual_change": _clean_bool(row.get("ManualChange")),
            "change_field": _clean_scalar(row.get("ChangeField")),
            "original": _clean_scalar(row.get("Original")),
            "change_reason": _clean_scalar(row.get("ChangeReason")),
            "modified_by": _clean_scalar(row.get("ModifiedBy")),
            "modified_date": _clean_scalar(row.get("ModifiedDate")),
            "upload_timestamp": _clean_scalar(row.get("UploadTimestamp")),
        }

        Center.upsert(session, data)
        logger.info("Upsert Center site_id=%s (%s)", site_id, title)

        # -------------------------
        # 2) Center ↔ Landlord links
        # -------------------------
        landlord_links = []

        landlord_id_1 = _clean_landlord_id(row.get("LandlordID"))
        if landlord_id_1 is not None:
            ownership_1 = _clean_ownership_pct(row.get("Ownership%"))
            landlord_links.append((landlord_id_1, ownership_1))

        landlord_id_2 = _clean_landlord_id(row.get("LandlordID2"))
        if landlord_id_2 is not None:
            ownership_2 = _clean_ownership_pct(row.get("CoOwnership%"))
            landlord_links.append((landlord_id_2, ownership_2))

        # First, we delete old links to ensure that the spreadsheet
        # completely replaces the previous state of that center.
        session.query(CenterLandlord).filter(
            CenterLandlord.site_id == site_id
        ).delete(synchronize_session=False)

        if landlord_links:
            # Recreates up to two links (one per landlord found in the row)
            for landlord_id, ownership_pct in landlord_links:
                cl_data = {
                    "site_id": site_id,
                    "landlord_id": landlord_id,
                    "ownership_pct": ownership_pct,
                }
                CenterLandlord.upsert(session, cl_data)
            logger.info(
                "Updated %d CenterLandlord links for site_id=%s",
                len(landlord_links),
                site_id,
            )
        else:
            # If there are no landlords in the row, we only ensure that no old links remain.
            session.commit()
            logger.info(
                "No landlords for site_id=%s; old links removed (if any).",
                site_id,
            )