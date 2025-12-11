import pandas as pd

from models import QualityReport

REQUIRED_FIELDS = ["ChainId", "LastUpdate", "Latitude", "Longitude", "Status"]

def generate_quality_report_and_save(df_collection_csv, collection_id, output_name):
    rows = generate_quality_report(df_collection_csv)
    result_df = pd.DataFrame(rows)
    save_rows_in_database(rows, collection_id, output_name)
    
    return result_df.to_csv(output_name, index=False)


def generate_quality_report(df_collection_csv):
    rows = []

    for idx, row in df_collection_csv.iterrows():

        validation_result, blank_cols, invalid_cols = validate_row(row)

        rows.append({
            "RowNumber": idx + 1,
            "ChainId": row.get("ChainId", None),
            "LastUpdate": row.get("LastUpdate", None),
            "ValitationResult": validation_result,
            "InvalidColumns": ", ".join(invalid_cols),
            "BlankColumns": ", ".join(blank_cols)
        })

    return rows


def save_rows_in_database(session, rows, collection_id, file_name):
    for row in rows:
        quality_report = get_quality_report_object(row, collection_id, file_name)
        QualityReport.upsert(session=session, data=quality_report)  


def get_quality_report_object(row, collection_id, file_name):
    quality_report_data = {
        "file_name": file_name,
        "collection_id": collection_id,
        "row_number": row.get("RowNumber"),
        "chain_id": row.get("ChainId"),
        "scrape_date": row.get("LastUpdate"),
        "valitation_result": row.get("ValitationResult"),
        "invalid_columns": row.get("InvalidColumns", ""),
        "blank_columns": row.get("BlankColumns", "")
    }

    return quality_report_data


def validate_row(row):

    blank_cols = []
    invalid_cols = []

    for col, value in row.items():
        if is_empty(value):
            blank_cols.append(col)

    if "Latitude" in row and not is_empty(row["Latitude"]):
        if is_invalid_number(row["Latitude"]):
            invalid_cols.append("Latitude")

    if "Longitude" in row and not is_empty(row["Longitude"]):
        if is_invalid_number(row["Longitude"]):
            invalid_cols.append("Longitude")

    has_required_blank = any(col in REQUIRED_FIELDS for col in blank_cols)
    has_required_invalid = any(col in REQUIRED_FIELDS for col in invalid_cols)

    if has_required_blank or has_required_invalid:
        validation_result = "INVALID"

    else:
        if blank_cols or invalid_cols:
            validation_result = "VALID-FOR-NECESSARY-FIELDS"
        else:
            validation_result = "VALID"

    return validation_result, blank_cols, invalid_cols


def is_empty(value):
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def is_invalid_number(value):
    if is_empty(value):
        return False
    try:
        float(value)
        return False
    except:
        return True

