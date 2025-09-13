"""
Main script to load media plan data and upsert to BigQuery.
"""
import sys
import os
import logging
import argparse
import pandas as pd

# Add the project root directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import get_media_plan_data
from database.bq_data_loader import load_dataframe_to_bigquery
from config import MEDIA_PLAN_DATA_TABLE

def main():
	parser = argparse.ArgumentParser(description="Load media plan data and upsert to BigQuery")
	parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
	parser.add_argument("--start-date", type=str, help="Filter records with StartDate >= this date (YYYY-MM-DD)")
	args = parser.parse_args()

	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	
	if args.dry_run:
		logging.info("DRY RUN MODE: No actual changes will be made")
	
	try:
		# Fetch media plan data from SQL Server
		logging.info("Fetching media plan data from SQL Server...")
		if args.start_date:
			logging.info(f"Filtering records with StartDate >= {args.start_date}")
		
		df = get_media_plan_data(start_date=args.start_date)
		
		if df is None:
			logging.error("get_media_plan_data returned None")
			return 1
		
		if df.empty:
			logging.warning("No media plan data returned (empty DataFrame).")
			return 0  # Not an error, just no data
		
		logging.info(f"Fetched {len(df)} rows from SQL Server.")
		logging.info(f"DataFrame columns: {list(df.columns)}")
		
		# Define key columns for upsert (adjust as needed for your schema)
		key_columns = [
			"media_plan_id", "estimate_id", "order_number", "order_line_number"
		]
		
		# Check if key columns exist in the DataFrame
		missing_keys = [col for col in key_columns if col not in df.columns]
		if missing_keys:
			logging.error(f"Key columns missing from DataFrame: {missing_keys}")
			logging.info(f"Available columns: {list(df.columns)}")
			return 1
		
		# Convert key columns to appropriate types for BigQuery
		logging.info("Converting key columns to appropriate types...")
		try:
			# Convert numeric key columns to integers, handling NaN values
			for key_col in key_columns:
				if key_col in ['media_plan_id', 'estimate_id', 'order_number', 'order_line_number']:
					# Convert to numeric first, then to nullable integer
					df[key_col] = pd.to_numeric(df[key_col], errors='coerce')
					# Convert to integer, replacing NaN with 0 or appropriate default
					df[key_col] = df[key_col].fillna(0).astype('int64')
					logging.info(f"Converted {key_col} to int64")
			
			# Handle problematic numeric columns that BigQuery can't determine types for
			numeric_columns = ['plan_agency_fee', 'plan_units', 'plan_net_amount', 'plan_bill_amount', 
							   'plan_clicks', 'plan_impressions']
			for col in numeric_columns:
				if col in df.columns:
					df[col] = pd.to_numeric(df[col], errors='coerce')
					# Fill NaN with 0 for numeric columns
					df[col] = df[col].fillna(0)
					logging.info(f"Converted {col} to numeric")
					
		except Exception as e:
			logging.error(f"Error converting columns: {str(e)}")
			return 1
		
		# Check for duplicate key combinations
		logging.info("Checking for duplicate key combinations...")
		duplicate_mask = df.duplicated(subset=key_columns, keep=False)
		duplicate_count = duplicate_mask.sum()
		
		if duplicate_count > 0:
			logging.warning(f"Found {duplicate_count} rows with duplicate key combinations")
			
			# Show some examples of duplicates
			duplicate_rows = df[duplicate_mask][key_columns + ['media_plan_description', 'start_date', 'end_date']].head(10)
			logging.warning(f"Sample duplicate rows:\n{duplicate_rows}")
			
			# Remove duplicates by keeping the first occurrence
			original_count = len(df)
			df = df.drop_duplicates(subset=key_columns, keep='first')
			final_count = len(df)
			
			logging.info(f"Removed {original_count - final_count} duplicate rows. Final count: {final_count}")
		else:
			logging.info("No duplicate key combinations found")
		
		logging.info(f"Using key columns for upsert: {key_columns}")

		# Upsert to BigQuery
		logging.info("Starting BigQuery upsert...")
		result = load_dataframe_to_bigquery(
			df=df,
			table_name=MEDIA_PLAN_DATA_TABLE,
			mode="upsert",
			key_columns=key_columns,
			snake_case=False,
			remove_quotes=True,
			clean_data_types=True,
			detect_types=True,
			dry_run=args.dry_run,
			validate_only=False
		)
		
		if result == 0:
			logging.info("Upsert to BigQuery completed successfully.")
		else:
			logging.error(f"Upsert to BigQuery failed with code {result}.")
		
		return result
		
	except Exception as e:
		logging.error(f"Unexpected error occurred: {str(e)}", exc_info=True)
		return 1

if __name__ == "__main__":
	exit_code = main()
	sys.exit(exit_code)
