import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd


# Load environment variables
load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create SQLAlchemy engine
#engine = create_engine(f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
engine = create_engine(
    f"mssql+pyodbc://{DB_SERVER}/{DB_DATABASE}?trusted_connection=yes&driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no",
    fast_executemany=True  # This remains a separate parameter
)

def get_media_plan_data(start_date=None, end_date=None):
    query = """
        SELECT
            MediaPlanID as media_plan_id,
            MediaPlanDescription as media_plan_description,
            OfficeCode as office_code,
            OfficeName as office_name,
            ClientCode as client_code,
            ClientName as client_name,
            DivisionCode as division_code,
            DivisionName as division_name,
            ProductCode as product_code,
            ProductName as product_name,
            PlanStartDate as plan_start_date,
            PlanEndDate as plan_end_date,
            CampaignID as campaign_id,
            CampaignName as campaign_name,
            EstimateID as estimate_id,
            SalesClassCode as sales_class_code,
            SalesClassDescription as sales_class_description,
            BuyerCode as buyer_code,
            EstimateName as estimate_name,
            OrderVendorCode as order_vendor_code,
            EstimateVendor as estimate_vendor,
            PlacementURL as placement_url,
            OrderNumber as order_number,
            OrderLineNumber as order_line_number,
            media_plan_dtl_level_line_data_id,
            StartDate as start_date,
            EndDate as end_date,
            PlanNetAmount as plan_net_amount,
            PlanBillAmount as plan_bill_amount,
            PlanAgencyFee as plan_agency_fee,
            PlanUnits as plan_units,
            PlanClicks as plan_clicks,
            PlanImpressions as plan_impressions,
            PlatformCampaignID as platform_campaign_id,
            PlatformAccountID as platform_account_id
    FROM SWD_V_MEDIA_PLAN

    """

    where_clauses = []
    params = []

    print(f"Entering get_campaign_data - StartDate: {start_date}, EndDate: {end_date}")

    # Only add WHERE conditions if parameters exist
    if start_date:
        where_clauses.append("StartDate >= ?")
        params.append(start_date)

    if end_date:
        where_clauses.append("EndDate <= ?")
        params.append(end_date)

    if where_clauses:
        query += " WHERE not OrderNumber is null AND " + " AND ".join(where_clauses)
    else:
        query += " WHERE not OrderNumber is null"

    print(f"Final SQL Query: {query}")
    print(f"SQL Parameters: {params}")

    # ✅ Convert list to a tuple inside a list
    return pd.read_sql(query, engine, params=[tuple(params)] if params else None)


def insert_digital_result(row, placement_id):
    insert_sql = """
        INSERT INTO DIGITAL_RESULTS (
            MEDIA_PLAN_ID,
            CL_CODE,
            DIV_CODE,
            PRD_CODE,
            MEDIA_PLAN_DTL_ID,
            VN_CODE,
            PLACEMENT_URL,
            PLACEMENT_ID
        )
        VALUES (
            :media_plan_id,
            :cl_code,
            :div_code,
            :prd_code,
            :media_plan_dtl_id,
            :vn_code,
            :placement_url,
            :placement_id
        )
    """

    params = {
        "media_plan_id": row["MediaPlanID"],
        "cl_code": row["ClientCode"],
        "div_code": row["DivisionCode"],
        "prd_code": row["ProductCode"],
        "media_plan_dtl_id": row["MediaPlanDtlID"],
        "vn_code": row["OrderVendorCode"],
        "placement_url": row["PlacementURL"],
        "placement_id": placement_id
    }

    with engine.begin() as conn:
        conn.execute(insert_sql, params)
