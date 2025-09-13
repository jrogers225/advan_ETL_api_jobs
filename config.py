"""Configuration constants for basisDSP package."""
import os

# API configuration
API_BASE = os.getenv("BASIS_API_BASE", "https://api.sitescout.com")
API_VERSION = os.getenv("BASIS_API_VERSION", "v2")

# Request throttling
REQUEST_SLEEP = 0.5  # seconds to sleep between API requests

# API endpoints
ENDPOINT_BRANDS = f"{API_BASE}/{API_VERSION}/advertisers/{{advertiser_id}}/brands"
ENDPOINT_CAMPAIGN_GROUPS = f"{API_BASE}/{API_VERSION}/advertisers/{{advertiser_id}}/brands/{{brand_id}}/campaignGroups"

# BigQuery tables used in the ETL process
# Only the table name is needed here; project and dataset are configured via database.bigquery_init
BASIS_BASIC_REPORT_TABLE = 'basis_basic_report_stg'
BASIS_CONVERSIONS_REPORT_TABLE = 'basis_conversions_report_stg'
MEDIA_PLAN_DATA_TABLE = 'media_plan_data_stg'
