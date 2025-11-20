import pandas as pd

REQUIRED_FIELDS = ["ChainId", "LastUpdate", "Latitude", "Longitude", "Status"]


def generate_quality_report(df_collection_csv, output_name):
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

    result_df = pd.DataFrame(rows)
    return result_df.to_csv(output_name, index=False)


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

