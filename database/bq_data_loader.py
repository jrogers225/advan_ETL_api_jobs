r"""BigQuery Data Loader Utility

Load a CSV file or pandas DataFrame into an existing BigQuery table with one of three modes:
    - replace: truncate and reload entire table
    - append: append rows to existing table
    - upsert: merge rows on provided key column(s)

Features:
    * Optional snake_case conversion of column names
    * Cleans currency ("$123.45") and percentage ("%98.6") strings
    * Removes surrounding single quotes from values
    * Data validation against existing table schema (table must already exist)
    * Upsert implemented via staging table + MERGE

Command line examples:
    python bq_data_loader.py --csv data/RevenuePivotData.csv --table media_plan_data --mode replace
    python bq_data_loader.py --csv data/new_rows.csv --table media_plan_data --mode append
    python bq_data_loader.py --csv data/updates.csv --table media_plan_data --mode upsert --key-columns id

Environment variables (.env or environment):
    BIGQUERY_PROJECT_ID
    BIGQUERY_DATASET_ID
    GOOGLE_APPLICATION_CREDENTIALS (optional if using default credentials)

Return codes:
    0 success
    1 validation / usage error
    2 BigQuery error
"""
from __future__ import annotations
import os
import sys
import argparse
import logging
import uuid
import time
import re
from typing import List, Optional, Dict
import pandas as pd
from datetime import datetime, date as _date

try:
    from google.api_core.exceptions import NotFound  # type: ignore
except Exception:  # pragma: no cover
    NotFound = Exception  # fallback

# Ensure project root (parent of this file's directory) is on sys.path so 'database' package resolves
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import isolated BigQuery initialization (avoids pulling in SQL Server dependencies)
try:
    from database.bigquery_init import (
        get_bigquery_client,
        BIGQUERY_PROJECT_ID,
        BIGQUERY_DATASET_ID,
        BIGQUERY_AVAILABLE,
    )
except Exception as import_err:  # pragma: no cover
    raise ImportError(
        "Failed to import BigQuery init module. Ensure database/bigquery_init.py exists."
    ) from import_err

try:
    from google.cloud import bigquery
except Exception:
    bigquery = None  # type: ignore

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def to_snake_case(name: str) -> str:
    """Convert a string to snake_case.
    
    Examples:
        'CamelCase' -> 'camel_case'
        'Already_Snake' -> 'already_snake'
        'Mixed Case With Spaces' -> 'mixed_case_with_spaces'
        'UPPER_CASE' -> 'upper_case'
        'numbers123AndText' -> 'numbers123_and_text'
    """
    # Replace spaces and hyphens with underscores
    name = re.sub(r'[-\s]+', '_', name)
    
    # Insert underscore before uppercase letters that follow lowercase letters or digits
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    
    # Convert to lowercase
    name = name.lower()
    
    # Remove any double underscores
    name = re.sub(r'_+', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    return name


def clean_and_convert_data_types(df: pd.DataFrame, key_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Clean and convert data types for better BigQuery compatibility.
    
    This function handles common data formatting issues like:
    - Currency values (e.g., '$123.45' -> 123.45)
    - Percentage values (e.g., '%98.6' -> 98.6)
    - Numeric strings that should be numbers
    
    Args:
        df: pandas DataFrame to process
        key_columns: List of key column names to protect from type conversion
        
    Returns:
        DataFrame with cleaned and converted data types
    """
    df_converted = df.copy()
    conversion_log = []

    # Convert numeric-like strings to numbers except protected key columns
    def _norm(s: str) -> str:
        s = str(s) if s is not None else ''
        s = s.strip().lower()
        return re.sub(r'[^a-z0-9]', '', s)

    # Protect key columns from type conversion to preserve their original format
    protected_norms = set()
    if key_columns:
        for key_col in key_columns:
            protected_norms.add(_norm(key_col))

    for col in df_converted.columns:
        if _norm(col) in protected_norms:
            continue
        if df_converted[col].dtype == 'object' or pd.api.types.is_string_dtype(df_converted[col]):
            non_null_mask = df_converted[col].notna()
            if not non_null_mask.any():
                continue
            sample_values = df_converted[col][non_null_mask].head(10).astype(str)

            currency_pattern = re.compile(r'^\$(\d+\.?\d*)$')
            if all(currency_pattern.match(v) for v in sample_values):
                try:
                    df_converted[col] = df_converted[col].astype(str).str.replace('$', '', regex=False).astype(float)
                    conversion_log.append(f"'{col}': currency -> float")
                    continue
                except (ValueError, TypeError):
                    pass

            percentage_pattern = re.compile(r'^%(\d+\.?\d*)$')
            if all(percentage_pattern.match(v) for v in sample_values):
                try:
                    df_converted[col] = df_converted[col].astype(str).str.replace('%', '', regex=False).astype(float)
                    conversion_log.append(f"'{col}': percentage -> float")
                    continue
                except (ValueError, TypeError):
                    pass

            def is_numeric_string(val: str) -> bool:
                try:
                    float(val)
                    return True
                except ValueError:
                    return False

            if all(is_numeric_string(v) for v in sample_values):
                try:
                    def is_integer_string(val: str) -> bool:
                        try:
                            float_val = float(val)
                            return float_val.is_integer()
                        except ValueError:
                            return False
                    if all(is_integer_string(v) for v in sample_values):
                        df_converted[col] = df_converted[col].astype(str).astype(int)
                        conversion_log.append(f"'{col}': string -> int")
                    else:
                        df_converted[col] = df_converted[col].astype(str).astype(float)
                        conversion_log.append(f"'{col}': string -> float")
                except (ValueError, TypeError):
                    pass
    
    if conversion_log:
        logging.info(f"Data type conversions applied: {', '.join(conversion_log)}")
    return df_converted


def remove_surrounding_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """Remove single quotes surrounding field values in string columns.
    
    This function removes single quotes that surround entire field values,
    but preserves quotes that appear within the content of fields.
    
    Args:
        df: pandas DataFrame to process
        
    Returns:
        DataFrame with quotes removed from string values
    """
    df_cleaned = df.copy()
    quote_removed_count = 0
    columns_processed = []
    
    for col in df_cleaned.columns:
        # Only process object/string columns
        if df_cleaned[col].dtype == 'object' or pd.api.types.is_string_dtype(df_cleaned[col]):
            # Convert to string and handle nulls
            original_series = df_cleaned[col].astype(str)
            
            # Remove single quotes only if they surround the entire value
            # Pattern: starts and ends with single quote
            cleaned_series = original_series.str.replace(r"^'(.*)'$", r"\1", regex=True)
            
            # Count how many values were actually changed
            changes_in_col = (original_series != cleaned_series).sum()
            if changes_in_col > 0:
                df_cleaned[col] = cleaned_series
                quote_removed_count += changes_in_col
                columns_processed.append(col)
                
                # Convert back to original null values
                null_mask = df[col].isna()
                if null_mask.any():
                    df_cleaned.loc[null_mask, col] = pd.NA
    
    if quote_removed_count > 0:
        logging.info(f"Removed surrounding single quotes from {quote_removed_count} values across {len(columns_processed)} columns: {columns_processed}")
    
    return df_cleaned


def convert_column_names_to_snake_case(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """Convert DataFrame column names to snake_case.
    
    Returns:
        tuple: (DataFrame with renamed columns, mapping of old_name -> new_name)
    """
    column_mapping = {}
    new_columns = []
    
    for col in df.columns:
        snake_col = to_snake_case(col)
        column_mapping[col] = snake_col
        new_columns.append(snake_col)
    
    # Create new DataFrame with renamed columns
    df_renamed = df.copy()
    df_renamed.columns = new_columns
    
    return df_renamed, column_mapping


def load_csv_to_dataframe(csv_path: str, detect_types: bool = True) -> pd.DataFrame:
    """Load CSV file into a pandas DataFrame.
    
    Args:
        csv_path: Path to CSV file
        detect_types: Whether to let pandas infer dtypes vs treat everything as string
        
    Returns:
        pandas DataFrame
    """
    read_kwargs = {
        "header": 0,  # Use first row as column names
    }
    if not detect_types:
        read_kwargs["dtype"] = str
    
    df = pd.read_csv(csv_path, **read_kwargs)
    
    # Clean column names (remove any leading/trailing spaces)
    df.columns = df.columns.str.strip()
    
    logging.info(f"Loaded CSV '{csv_path}' with {len(df)} rows and {len(df.columns)} columns")
    # logging.info(f"Column names: {list(df.columns)}")
    
    return df


def parse_args(argv: List[str]):
    parser = argparse.ArgumentParser(description="Load a CSV into BigQuery with replace/append/upsert modes.")
    parser.add_argument("--csv", required=True, help="Path to input CSV file")
    parser.add_argument("--table", required=True, help="Target BigQuery table name (without project/dataset)")
    parser.add_argument("--mode", required=True, choices=["replace", "append", "upsert"], help="Load mode")
    parser.add_argument("--key-columns", help="Comma separated list of key column(s) for upsert mode")
    parser.add_argument("--batch-size", type=int, default=10000, help="Row batch size for load (informational)")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without executing")
    parser.add_argument("--validate-only", action="store_true", help="Only validate data against schema, don't load")
    parser.add_argument("--detect-types", action="store_true", help="Let pandas infer dtypes (default) vs treat everything as string")
    parser.add_argument("--snake-case", action="store_true", help="Convert column names to snake_case for BigQuery schema")
    parser.add_argument("--remove-quotes", action="store_true", help="Remove single quotes surrounding field values")
    parser.add_argument("--clean-data-types", action="store_true", help="Clean and convert currency/percentage/numeric data types")
    parser.add_argument("--time", action="store_true", help="Print elapsed time")
    return parser.parse_args(argv)


def validate_args(args) -> bool:
    if not os.path.exists(args.csv):
        logging.error(f"CSV file not found: {args.csv}")
        return False
    if args.mode == "upsert" and not args.key_columns:
        logging.error("--key-columns required for upsert mode")
        return False
    return True


def ensure_keys(df: pd.DataFrame, keys: List[str]):
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"Key column(s) missing from DataFrame: {missing}")


def get_table(client, full_table_id: str):
    """Return table or None if not found."""
    try:
        return client.get_table(full_table_id)
    except NotFound:  # type: ignore
        return None
    except Exception as e:
        raise RuntimeError(f"Failed to fetch table {full_table_id}: {e}")


def infer_bq_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "INT64"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT64"
    if pd.api.types.is_bool_dtype(series):
        return "BOOL"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    
    # For string/object columns, sample non-null values for pattern analysis
    non_null = series.dropna().head(20).astype(str)
    if len(non_null) == 0:
        return "STRING"
    
    # Check for date patterns
    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"]
    def looks_like_date(val: str) -> bool:
        for fmt in date_formats:
            try:
                datetime.strptime(val, fmt)
                return True
            except ValueError:
                continue
        return False
    
    if all(looks_like_date(v) for v in non_null):
        return "DATE"
    
    # Check for currency patterns (e.g., $123.45, $0.000000)
    currency_pattern = re.compile(r'^\$\d+\.?\d*$')
    if all(currency_pattern.match(v) for v in non_null):
        return "FLOAT64"
    
    # Check for percentage patterns (e.g., %98.6553, %0.0000)
    percentage_pattern = re.compile(r'^%\d+\.?\d*$')
    if all(percentage_pattern.match(v) for v in non_null):
        return "FLOAT64"
    
    # Check for plain numeric strings that could be converted to numbers
    def is_numeric_string(val: str) -> bool:
        try:
            float(val)
            return True
        except ValueError:
            return False
    
    if all(is_numeric_string(v) for v in non_null):
        # Check if all values look like integers
        def is_integer_string(val: str) -> bool:
            try:
                float_val = float(val)
                return float_val.is_integer()
            except ValueError:
                return False
        
        if all(is_integer_string(v) for v in non_null):
            return "INT64"
        else:
            return "FLOAT64"
    
    return "STRING"


def infer_bq_schema(df: pd.DataFrame):
    schema = []
    if bigquery is None:  # pragma: no cover
        return schema
    for col in df.columns:
        bq_type = infer_bq_type(df[col])
        schema.append(bigquery.SchemaField(col, bq_type))
    return schema


ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _looks_like_iso_date_series(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False
    sample = non_null.head(20).astype(str)
    return all(bool(ISO_DATE_RE.match(v)) for v in sample)


def coerce_date_columns(df: pd.DataFrame, date_columns: List[str]):
    """Convert columns that are YYYY-MM-DD strings to datetime.date objects.

    Only processes columns listed in date_columns and where strings match pattern.
    """
    changed = []
    for col in date_columns:
        if col not in df.columns:
            continue
        s = df[col]
        # Skip if already datetime-like
        if pd.api.types.is_datetime64_any_dtype(s) or all(hasattr(v, 'year') for v in s.dropna().head(3)):
            continue
        if _looks_like_iso_date_series(s):
            try:
                parsed = pd.to_datetime(s, errors='coerce').dt.date
                # If parsing produced many nulls, skip
                if parsed.notna().sum() >= s.notna().sum() * 0.9:  # allow a few failures
                    df[col] = parsed
                    changed.append(col)
            except Exception:  # pragma: no cover
                continue
    if changed:
        logging.info(f"Coerced columns to DATE: {changed}")


def validate_data_against_schema(df: pd.DataFrame, table_schema, table_name: str) -> tuple[bool, List[str]]:
    """Validate DataFrame data types against BigQuery table schema.
    
    Args:
        df: DataFrame to validate
        table_schema: BigQuery table schema (list of SchemaField objects)
        table_name: Table name for error reporting
        
    Returns:
        tuple: (is_valid, list_of_error_messages)
    """
    errors = []
    
    # Create mapping of column name to expected BigQuery type
    schema_types = {field.name: field.field_type for field in table_schema}
    
    # Check if we have data to validate
    if df.empty:
        return True, []
    
    # Sample first few rows for validation
    sample_df = df.head(5)
    
    for col_name, bq_type in schema_types.items():
        if col_name not in df.columns:
            continue  # Column not in DataFrame (might be optional)
            
        col_data = sample_df[col_name]
        non_null_data = col_data.dropna()
        
        if len(non_null_data) == 0:
            continue  # No data to validate
            
        # Check compatibility based on BigQuery type
        sample_values = non_null_data.head(3).tolist()
        
        if bq_type == "STRING":
            # STRING can accept anything, but check for reasonable string conversion
            try:
                for val in sample_values:
                    str(val)
            except Exception as e:
                errors.append(f"Column '{col_name}' (STRING): Cannot convert value to string: {e}")
                
        elif bq_type == "INT64":
            # Check if values can be converted to integers
            for val in sample_values:
                try:
                    if pd.isna(val):
                        continue
                    # Try to convert to int
                    if isinstance(val, (int, float)):
                        if isinstance(val, float) and not val.is_integer():
                            errors.append(f"Column '{col_name}' (INT64): Float value {val} cannot be converted to integer")
                    else:
                        int(float(str(val)))
                except (ValueError, TypeError) as e:
                    errors.append(f"Column '{col_name}' (INT64): Cannot convert '{val}' to integer: {e}")
                    
        elif bq_type == "FLOAT64":
            # Check if values can be converted to floats
            for val in sample_values:
                try:
                    if pd.isna(val):
                        continue
                    float(str(val))
                except (ValueError, TypeError) as e:
                    errors.append(f"Column '{col_name}' (FLOAT64): Cannot convert '{val}' to float: {e}")
                    
        elif bq_type == "BOOL":
            # Check if values are boolean-like
            for val in sample_values:
                if pd.isna(val):
                    continue
                if not isinstance(val, (bool, int)) and str(val).lower() not in ['true', 'false', '1', '0']:
                    errors.append(f"Column '{col_name}' (BOOL): Cannot convert '{val}' to boolean")
                    
        elif bq_type == "DATE":
            # Check if values can be parsed as dates
            for val in sample_values:
                if pd.isna(val):
                    continue
                try:
                    if hasattr(val, 'year'):  # Already a date-like object
                        continue
                    # Try to parse as date string
                    val_str = str(val)
                    if not re.match(r'^\d{4}-\d{2}-\d{2}$', val_str):
                        # Try other common date formats
                        from datetime import datetime
                        datetime.strptime(val_str, '%Y-%m-%d')
                except (ValueError, TypeError) as e:
                    errors.append(f"Column '{col_name}' (DATE): Cannot parse '{val}' as date: {e}")
                    
        elif bq_type == "TIMESTAMP":
            # Check if values can be parsed as timestamps
            for val in sample_values:
                if pd.isna(val):
                    continue
                try:
                    if pd.api.types.is_datetime64_any_dtype(type(val)):
                        continue
                    pd.to_datetime(val)
                except (ValueError, TypeError) as e:
                    errors.append(f"Column '{col_name}' (TIMESTAMP): Cannot parse '{val}' as timestamp: {e}")
    
    # Check for extra columns in DataFrame that don't exist in schema
    extra_cols = set(df.columns) - set(schema_types.keys())
    if extra_cols:
        errors.append(f"DataFrame has columns not in table schema: {list(extra_cols)}")
    
    is_valid = len(errors) == 0
    return is_valid, errors


def prompt_create_table(client, full_table_id: str, df: pd.DataFrame, dry_run: bool = False):
    schema = infer_bq_schema(df)
    print("\nTable does not exist:", full_table_id)
    print("Proposed schema (column -> type):")
    for field in schema:
        sample_val = next((str(v) for v in df[field.name].dropna().head(1).tolist()), "")
        if len(sample_val) > 40:
            sample_val = sample_val[:37] + "..."
        print(f"  {field.name:30} {field.field_type:12} sample='{sample_val}'")
    if dry_run:
        print("[DRY-RUN] Skipping creation (would create table above).")
        return schema, None
    resp = input("Create table with this schema? [y/N]: ").strip().lower()
    if resp != 'y':
        raise SystemExit("Aborted by user; table creation declined.")
    tbl = bigquery.Table(full_table_id, schema=schema)
    created = client.create_table(tbl)
    logging.info(f"Created table {created.full_table_id}")
    return schema, created


def load_dataframe_to_bq_table(client, df: pd.DataFrame, full_table_id: str, write_disposition: str, dry_run: bool = False):
    """Load DataFrame directly to BigQuery table."""
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    if dry_run:
        logging.info(f"[DRY-RUN] Would load {len(df)} rows into {full_table_id} (write_disposition={write_disposition})")
        return None
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    result = job.result()
    logging.info(f"Load job finished: {result.output_rows} rows written to {full_table_id}")
    return result


def perform_merge(client, staging_full_id: str, target_full_id: str, key_columns: List[str], df_columns: List[str], dry_run: bool = False, cast_key_columns_to_string: bool = False, cast_columns: Optional[Dict[str, str]] = None):
    """Perform MERGE from staging into target.

    If cast_key_columns_to_string is True, both sides of each key equality are
    CAST(... AS STRING) to avoid BigQuery errors comparing INT64 to STRING.
    This is a defensive fallback when schemas or loaded types diverge.
    """
    # Build ON clause (optionally casting to STRING)
    if cast_key_columns_to_string:
        on_parts = [f"CAST(T.{col} AS STRING)=CAST(S.{col} AS STRING)" for col in key_columns]
    else:
        on_parts = [f"T.{col}=S.{col}" for col in key_columns]
    on_clause = " AND ".join(on_parts)
    # Columns to update (exclude keys)
    update_cols = [c for c in df_columns if c not in key_columns]
    if not update_cols:
        logging.warning("No non-key columns to update; merge will only insert missing rows")
    # Optionally cast non-key columns from staging types to target types
    def maybe_cast(col: str) -> str:
        if cast_columns and col in cast_columns:
            tgt_type = cast_columns[col].upper()
            # Special handling for temporal types: normalize whitespace and parse multiple common formats
            if tgt_type in ("TIMESTAMP", "DATETIME", "DATE"):
                # Normalize whitespace and trim; ensure string for parsing
                norm_expr = f"TRIM(REGEXP_REPLACE(CAST(S.{col} AS STRING), r'\\s+', ' '))"
                if tgt_type == "TIMESTAMP":
                    # Try AM/PM 12-hour, then 24-hour, then ISO T separator
                    return (
                        "COALESCE("
                        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%d %I:%M:%S %p', {norm_expr}), "
                        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {norm_expr}), "
                        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', {norm_expr})"
                        ")"
                    )
                if tgt_type == "DATETIME":
                    return (
                        "COALESCE("
                        f"SAFE.PARSE_DATETIME('%Y-%m-%d %I:%M:%S %p', {norm_expr}), "
                        f"SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', {norm_expr}), "
                        f"SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', {norm_expr})"
                        ")"
                    )
                if tgt_type == "DATE":
                    return (
                        "COALESCE("
                        f"SAFE.PARSE_DATE('%Y-%m-%d', {norm_expr}), "
                        f"SAFE.PARSE_DATE('%m/%d/%Y', {norm_expr})"
                        ")"
                    )
            return f"CAST(S.{col} AS {tgt_type})"
        return f"S.{col}"
    set_clause = ", ".join([f"{col}={maybe_cast(col)}" for col in update_cols]) or ""
    insert_cols = ", ".join(df_columns)
    insert_values = ", ".join([maybe_cast(c) for c in df_columns])

    merge_sql = f"""
MERGE `{target_full_id}` T
USING `{staging_full_id}` S
ON {on_clause}
WHEN MATCHED THEN UPDATE SET {set_clause if set_clause else on_clause.split(' AND ')[0]} -- fallback no-op update if only keys
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
""".strip()

    # logging.info(f"Executing MERGE with keys {key_columns}\nSQL:\n{merge_sql}")
    if dry_run:
        logging.info("[DRY-RUN] MERGE not executed")
        return None
    
    # Get row count before MERGE for comparison
    try:
        count_before_query = f"SELECT COUNT(*) as count FROM `{target_full_id}`"
        count_before_job = client.query(count_before_query)
        count_before = list(count_before_job.result())[0].count
    except Exception as e:
        logging.debug(f"Could not get row count before MERGE: {e}")
        count_before = None
    
    # Execute the MERGE
    query_job = client.query(merge_sql)
    result = query_job.result()
    
    # Get row count after MERGE
    try:
        count_after_query = f"SELECT COUNT(*) as count FROM `{target_full_id}`"
        count_after_job = client.query(count_after_query)
        count_after = list(count_after_job.result())[0].count
        
        if count_before is not None:
            rows_inserted = count_after - count_before
            rows_updated = len(df_columns) if hasattr(query_job, 'num_dml_affected_rows') and query_job.num_dml_affected_rows else 0
            if query_job.num_dml_affected_rows is not None:
                rows_updated = max(0, query_job.num_dml_affected_rows - rows_inserted)
            
            logging.info(f"MERGE completed - Rows inserted: {rows_inserted}, Rows updated: {rows_updated}, Total rows in table: {count_after}")
        else:
            logging.info(f"MERGE completed - Total rows in table: {count_after}")
            
    except Exception as e:
        logging.debug(f"Could not get row count after MERGE: {e}")
        # Log the number of rows affected by the MERGE operation
        if hasattr(query_job, 'num_dml_affected_rows') and query_job.num_dml_affected_rows is not None:
            logging.info(f"MERGE completed - Total rows affected: {query_job.num_dml_affected_rows}")
        else:
            logging.info("MERGE completed")


def drop_table_safely(client, full_table_id: str):
    try:
        client.delete_table(full_table_id, not_found_ok=True)
        logging.info(f"Dropped staging table {full_table_id}")
    except Exception as e:
        logging.warning(f"Failed to drop staging table {full_table_id}: {e}")


def load_dataframe_to_bigquery(
    df: pd.DataFrame,
    table_name: str,
    mode: str = "replace",
    key_columns: Optional[List[str]] = None,
    snake_case: bool = False,
    remove_quotes: bool = False,
    clean_data_types: bool = False,
    detect_types: bool = True,
    dry_run: bool = False,
    validate_only: bool = False,
    expected_types: Optional[Dict[str, str]] = None,
) -> int:
    """Load a pandas DataFrame into BigQuery with replace/append/upsert modes.
    
    Args:
        df: pandas DataFrame to load
        table_name: Target BigQuery table name (without project/dataset)
        mode: Load mode - "replace", "append", or "upsert"
        key_columns: List of key column names for upsert mode
        snake_case: Convert column names to snake_case
        remove_quotes: Remove single quotes surrounding field values
        clean_data_types: Clean and convert currency/percentage/numeric data types
        detect_types: Let pandas infer dtypes vs treat everything as string
        dry_run: Show actions without executing
        validate_only: Only validate data against schema, don't load
        
    Returns:
        0 for success, 1 for validation error, 2 for BigQuery error
        
    Example:
        import pandas as pd
        from database.bq_data_loader import load_dataframe_to_bigquery
        
        df = pd.read_csv('data.csv')
        result = load_dataframe_to_bigquery(
            df=df,
            table_name='my_table',
            mode='replace',
            snake_case=True,
            remove_quotes=True,
            clean_data_types=True
        )
    """
    start_ts = time.time()

    # Defensive: ensure df provided
    if df is None:
        logging.error("load_dataframe_to_bigquery received df=None")
        return 1
    if not BIGQUERY_AVAILABLE or bigquery is None:
        logging.error("BigQuery libraries not available. Install google-cloud-bigquery and pandas-gbq.")
        return 1
    # Validate inputs
    if mode not in ["replace", "append", "upsert"]:
        logging.error(f"Invalid mode '{mode}'. Must be one of: replace, append, upsert")
        return 1
        
    if mode == "upsert" and not key_columns:
        logging.error("key_columns required for upsert mode")
        return 1
        
    if df.empty:
        logging.error("DataFrame is empty")
        return 1

    try:
        client = get_bigquery_client()
    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}")
        return 2

    # Process DataFrame
    try:
        logging.debug(f"Input DataFrame type={type(df)} rows={getattr(df, 'shape', ['?','?'])[0]} cols={getattr(df, 'shape', ['?','?'])[1] if hasattr(df,'shape') else '?'}")
        df_processed = df.copy()
        logging.debug("DataFrame copy succeeded")
    except Exception as e:
        logging.error(f"Failed to copy DataFrame (type={type(df)}): {e}")
        return 1
    if not isinstance(df_processed, pd.DataFrame):
        logging.error(f"After copy step df_processed is not DataFrame (type={type(df_processed)})")
        return 1
    
    # Clean column names (remove any leading/trailing spaces)
    df_processed.columns = df_processed.columns.str.strip()
    
    # Remove surrounding single quotes from field values if requested
    if remove_quotes:
        try:
            logging.debug("Removing surrounding quotes from string columns")
            df_processed = remove_surrounding_quotes(df_processed)
        except Exception as e:
            logging.error(f"remove_surrounding_quotes failed: {e}")
            return 1
        if not isinstance(df_processed, pd.DataFrame):
            logging.error(f"remove_surrounding_quotes returned non-DataFrame type={type(df_processed)}")
            return 1
    
    # Clean and convert data types if requested (currency, percentages, etc.)
    if clean_data_types:
        try:
            logging.debug("Cleaning & converting data types")
            df_processed = clean_and_convert_data_types(df_processed, key_columns)
            logging.debug("clean_and_convert_data_types complete")
        except Exception as e:
            logging.error(f"clean_and_convert_data_types failed: {e}")
            return 1
        if not isinstance(df_processed, pd.DataFrame):
            logging.error(f"clean_and_convert_data_types returned non-DataFrame type={type(df_processed)}")
            return 1
    
    # Convert to snake_case if requested
    column_mapping = {}
    if snake_case:
        df_processed, column_mapping = convert_column_names_to_snake_case(df_processed)
        if column_mapping:
            # Show the mapping
            logging.info("Column name conversions:")
            for old, new in column_mapping.items():
                if old != new:
                    logging.info(f"  '{old}' -> '{new}'")
    else:
        # Create identity mapping
        column_mapping = {col: col for col in df_processed.columns}
    if not isinstance(df_processed, pd.DataFrame):
        logging.error(f"After snake_case/identity mapping df_processed type={type(df_processed)}")
        return 1
    
    # Convert dtypes if not detecting types
    if not detect_types:
        df_processed = df_processed.astype(str)
        if not isinstance(df_processed, pd.DataFrame):
            logging.error(f"astype(str) returned non-DataFrame type={type(df_processed)}")
            return 1
    
    try:
        logging.info(f"Processing DataFrame with {len(df_processed)} rows and {len(df_processed.columns)} columns")
        logging.info(f"Column names: {list(df_processed.columns)}")
        logging.debug(f"DataFrame head sample:\n{df_processed.head(3)}")
    except Exception as e:
        logging.error(f"Accessing DataFrame columns failed (df_processed type={type(df_processed)}): {e}")
        return 1

    full_table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}"
    table = get_table(client, full_table_id)
    
    if table is None:
        # Table doesn't exist - this is now an error instead of creating it
        logging.error(f"Table {full_table_id} does not exist. Please create the table first.")
        logging.error("Use BigQuery Console or bq command-line tool to create the table with the appropriate schema.")
        return 1
    
    # Get table schema and column names
    table_cols = [schema_field.name for schema_field in table.schema]
    
    # Validate DataFrame data against table schema
    logging.info(f"Validating data compatibility with table schema...")
    is_valid, validation_errors = validate_data_against_schema(df_processed, table.schema, table_name)
    
    if not is_valid:
        logging.error("Data validation failed against table schema:")
        for error in validation_errors:
            logging.error(f"  - {error}")
        logging.error("Please fix the data issues or adjust the table schema before loading.")
        return 1
    
    logging.info("✓ Data validation passed - all columns are compatible with table schema")
    
    # If expected_types provided (advisory), warn if they differ from target table schema
    if expected_types:
        try:
            exp = {str(k): str(v).upper() for k, v in expected_types.items()}
            tgt = {f.name: getattr(f, 'field_type', '').upper() for f in table.schema}
            diffs = [(c, exp[c], tgt.get(c, '')) for c in exp.keys() if tgt.get(c) and tgt.get(c) != exp[c]]
            if diffs:
                logging.warning("Type mismatches vs mapping (target schema wins):")
                for c, et, tt in diffs:
                    logging.warning(f"  {c}: mapping={et}, target={tt}")
        except Exception:
            pass

    # If validate_only is True, stop here
    if validate_only:
        logging.info("Validation complete. Data is ready for loading.")
        if dry_run:
            logging.info("Use --dry-run flag to see what loading operations would be performed.")
        return 0
    
    # If snake_case conversion was used, we need to check against the converted column names
    if snake_case:
        # Warn if existing table columns don't match expected snake_case names
        expected_table_cols = [to_snake_case(original) for original in column_mapping.keys()]
        mismatched = []
        for expected, actual in zip(expected_table_cols, table_cols):
            if expected != actual:
                mismatched.append(f"expected '{expected}' but table has '{actual}'")
        if mismatched:
            logging.warning(f"Table column names don't match snake_case expectations: {mismatched}")

    # Coerce DATE columns (only those whose schema type is DATE when table exists)
    date_cols_target: List[str] = [f.name for f in table.schema if getattr(f, 'field_type', '').upper() == 'DATE']
    if date_cols_target:
        coerce_date_columns(df_processed, date_cols_target)

    # Coerce float percentage/currency derived columns to Python Decimal when target schema expects NUMERIC/BIGNUMERIC
    from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
    numeric_fields = [f for f in table.schema if getattr(f, 'field_type', '').upper() in ('NUMERIC','BIGNUMERIC')]
    coerced_numeric = []
    for f in numeric_fields:
        col = f.name
        if col not in df_processed.columns:
            continue
        # Only coerce if current dtype is float to avoid touching ints/decimals already good
        if str(df_processed[col].dtype).startswith('float'):
            # Determine scale (NUMERIC default 9 fractional digits; BIGNUMERIC allow more but we cap at 18 here)
            scale = 9 if f.field_type.upper() == 'NUMERIC' else 18
            quantizer = Decimal('1.' + ('0'*scale)) if scale > 0 else Decimal('1')
            def _coerce(v):
                if v is None or (isinstance(v, float) and (v != v)):  # NaN check
                    return None
                try:
                    d = Decimal(str(v))
                    if scale > 0:
                        d = d.quantize(quantizer, rounding=ROUND_HALF_UP)
                    return d
                except (InvalidOperation, ValueError):
                    return None
            df_processed[col] = df_processed[col].apply(_coerce)
            coerced_numeric.append(col)
    #if coerced_numeric:
    #    logging.info(f"Coerced float columns to Decimal for NUMERIC/BIGNUMERIC schema: {coerced_numeric}")

    # Final check: Validate DataFrame columns are subset of table columns
    missing_cols = [c for c in df_processed.columns if c not in table_cols]
    if missing_cols:
        logging.error(f"DataFrame has columns not present in target table: {missing_cols}")
        if snake_case and column_mapping:
            logging.error("Note: Column names were converted to snake_case. Original DataFrame columns:")
            for original, converted in column_mapping.items():
                if converted in missing_cols:
                    logging.error(f"  Original: '{original}' -> Converted: '{converted}'")
        return 1

    if mode in ("replace", "append"):
        write_disposition = "WRITE_TRUNCATE" if mode == "replace" else "WRITE_APPEND"
        load_dataframe_to_bq_table(client, df_processed, full_table_id, write_disposition, dry_run=dry_run)
    else:  # upsert
        # If snake_case conversion was used, convert key column names too
        processed_key_columns = key_columns.copy() if key_columns else []
        if snake_case and key_columns:
            # Map original key column names to snake_case
            converted_key_columns = []
            for key in key_columns:
                # Find the snake_case equivalent
                snake_key = None
                for original, converted in column_mapping.items():
                    if original == key or converted == key:
                        snake_key = converted
                        break
                if snake_key is None:
                    snake_key = to_snake_case(key)  # fallback conversion
                converted_key_columns.append(snake_key)
            processed_key_columns = converted_key_columns
            logging.info(f"Key columns converted to snake_case: {processed_key_columns}")
        
        ensure_keys(df_processed, processed_key_columns)

        staging_name = f"{table_name}__staging_{uuid.uuid4().hex[:8]}"
        staging_full_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{staging_name}"

        # Create staging table with target schema
        if dry_run:
            logging.info(f"[DRY-RUN] Would create staging table {staging_full_id}")
        else:
            if table is None:
                logging.error("Cannot create staging table: target table does not exist")
                return 1
            # Build staging schema: use mapping-provided expected_types when supplied, otherwise target schema
            staging_schema = []
            if expected_types:
                overrides = {str(k): str(v).upper() for k, v in expected_types.items()}
                for f in table.schema:
                    new_type = overrides.get(f.name, getattr(f, 'field_type', 'STRING'))
                    try:
                        staging_schema.append(
                            bigquery.SchemaField(
                                name=f.name,
                                field_type=new_type,
                                mode=getattr(f, 'mode', 'NULLABLE'),
                                description=getattr(f, 'description', None),
                                fields=getattr(f, 'fields', ()),
                            )
                        )
                    except TypeError:
                        staging_schema.append(bigquery.SchemaField(f.name, new_type))
            staging_table = bigquery.Table(staging_full_id, schema=staging_schema or table.schema)
            client.create_table(staging_table)
            logging.info(f"Created staging table {staging_full_id}")

        # Load data into staging (truncate semantics)
        load_dataframe_to_bq_table(client, df_processed, staging_full_id, "WRITE_TRUNCATE", dry_run=dry_run)

        # MERGE
        # Detect key dtype mismatches (pandas vs schema) to decide on casting
        cast_keys = False
        schema_type_map = {f.name: getattr(f, 'field_type', '').upper() for f in table.schema}
        # Build staging type map to optionally cast non-key columns to target types
        staging_type_map = {}
        try:
            st_tbl = get_table(client, staging_full_id)
            if st_tbl:
                staging_type_map = {f.name: getattr(f, 'field_type', '').upper() for f in st_tbl.schema}
        except Exception:
            staging_type_map = {}
        for k in processed_key_columns:
            schema_t = schema_type_map.get(k, '')
            df_t = str(df_processed[k].dtype) if k in df_processed.columns else ''
            if schema_t in ('INTEGER', 'INT64') and df_t.startswith('object'):
                cast_keys = True
                break
            if schema_t == 'STRING' and df_t.startswith(('int64', 'Int64')):
                cast_keys = True
                break
        if cast_keys:
            logging.warning("Detected potential key type mismatch; casting key columns to STRING in MERGE predicate")
        # Determine per-column casts for non-key columns based on staging vs target types
        cast_columns: Dict[str, str] = {}
        if staging_type_map:
            for col in df_processed.columns:
                if col in processed_key_columns:
                    continue
                tgt_t = schema_type_map.get(col)
                stg_t = staging_type_map.get(col)
                if tgt_t and stg_t and tgt_t != stg_t:
                    cast_columns[col] = tgt_t
        perform_merge(
            client,
            staging_full_id,
            full_table_id,
            processed_key_columns,
            list(df_processed.columns),
            dry_run=dry_run,
            cast_key_columns_to_string=cast_keys,
            cast_columns=cast_columns if cast_columns else None,
        )

        # Drop staging
        if not dry_run:
            drop_table_safely(client, staging_full_id)

    elapsed = time.time() - start_ts
    logging.info(f"DataFrame load completed in {elapsed:.2f}s")

    return 0


def run(args) -> int:
    """Main execution function for command line usage.
    
    This function now uses the unified DataFrame approach:
    1. Load CSV to DataFrame
    2. Use load_dataframe_to_bigquery for all processing
    """
    start_ts = time.time()

    if not BIGQUERY_AVAILABLE or bigquery is None:
        logging.error("BigQuery libraries not available. Install google-cloud-bigquery and pandas-gbq.")
        return 1

    if not validate_args(args):
        return 1

    try:
        # Load CSV to DataFrame
        df = load_csv_to_dataframe(args.csv, detect_types=args.detect_types)
        
        # Parse key columns if provided
        key_columns = None
        if args.key_columns:
            key_columns = [c.strip() for c in args.key_columns.split(",") if c.strip()]
        
        # Use the unified DataFrame loading function
        result = load_dataframe_to_bigquery(
            df=df,
            table_name=args.table,
            mode=args.mode,
            key_columns=key_columns,
            snake_case=args.snake_case,
            remove_quotes=args.remove_quotes,
            clean_data_types=args.clean_data_types,
            detect_types=args.detect_types,
            dry_run=args.dry_run,
            validate_only=args.validate_only
        )
        
        if args.time:
            elapsed = time.time() - start_ts
            logging.info(f"Total elapsed time: {elapsed:.2f}s")
        
        return result
        
    except Exception as e:
        logging.error(f"Error processing CSV file: {e}")
        return 2


def main():
    args = parse_args(sys.argv[1:])
    rc = run(args)
    sys.exit(rc)


if __name__ == "__main__":
    main()
