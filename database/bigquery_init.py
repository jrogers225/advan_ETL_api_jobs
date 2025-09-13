"""BigQuery initialization utilities isolated from other database connections.

Provides:
  - BIGQUERY_PROJECT_ID
  - BIGQUERY_DATASET_ID
  - BIGQUERY_AVAILABLE flag
  - get_bigquery_client()

Environment variables (loaded from .env if present):
  GOOGLE_APPLICATION_CREDENTIALS
  BIGQUERY_PROJECT_ID
  BIGQUERY_DATASET_ID
"""
from __future__ import annotations
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "revenue_plan_data")

BIGQUERY_AVAILABLE = False
bigquery = None  # type: ignore
service_account = None  # type: ignore

def _lazy_import():  # pragma: no cover (environment dependent)
    """Attempt to import BigQuery libraries only when first needed.

    Sets BIGQUERY_AVAILABLE flag. Returns (bigquery, service_account).
    """
    global BIGQUERY_AVAILABLE, bigquery, service_account
    if BIGQUERY_AVAILABLE and bigquery is not None:
        return bigquery, service_account
    try:
        from google.cloud import bigquery as _bq  # type: ignore
        from google.oauth2 import service_account as _sa  # type: ignore
        bigquery = _bq
        service_account = _sa
        BIGQUERY_AVAILABLE = True
    except Exception as e:
        BIGQUERY_AVAILABLE = False
        logging.debug(f"Deferred BigQuery import failed: {e}")
    return bigquery, service_account


def get_bigquery_client():
    """Create and return a BigQuery client using service account or default credentials.

    Raises:
        RuntimeError: if BigQuery libraries are not installed or client creation fails.
    """
    if not BIGQUERY_AVAILABLE:
        _lazy_import()
    if not BIGQUERY_AVAILABLE or bigquery is None:
        raise RuntimeError(
            "BigQuery libraries not installed or failed to import. Install with: pip install google-cloud-bigquery google-auth --upgrade"
        )
    try:
        if GOOGLE_APPLICATION_CREDENTIALS and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
            credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)  # type: ignore[arg-type]
            return bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)  # type: ignore[call-arg]
        return bigquery.Client(project=BIGQUERY_PROJECT_ID)  # type: ignore[call-arg]
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Failed to create BigQuery client: {e}") from e


__all__ = [
    "BIGQUERY_PROJECT_ID",
    "BIGQUERY_DATASET_ID",
    "BIGQUERY_AVAILABLE",
    "get_bigquery_client",
]

# Initialize BigQuery availability on module import
_lazy_import()
