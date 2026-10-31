"""Where the data comes from.

ACRA publishes its register of every business entity as 27 CSV files on data.gov.sg
(one per first letter of the name, plus 'others'). The files are full snapshots: each
monthly release replaces the last, and nothing says which rows changed.

SingStat publishes the Retail Sales Index and the Food & Beverage Services Index
through its Table Builder API.
"""
from __future__ import annotations

ACRA_COLLECTION_ID = "2"  # "ACRA Information on Corporate Entities"

DATA_GOV_API = "https://api-production.data.gov.sg/v2/public/api"
DATA_GOV_DOWNLOAD_API = "https://api-open.data.gov.sg/v1/public/api"
SINGSTAT_API = "https://tablebuilder.singstat.gov.sg/api/table"

SINGSTAT_TABLES = {
    "M602121": "retail_sales_index",  # Retail Sales Index, current prices, monthly
    "M602131": "fnb_services_index",  # F&B Services Index, current prices, monthly
}

# Columns the pipeline relies on. A release missing any of them is rejected.
ACRA_REQUIRED_COLUMNS = [
    "uen",
    "entity_name",
    "entity_type_description",
    "business_constitution_description",
    "company_type_description",
    "entity_status_description",
    "registration_incorporation_date",
    "postal_code",
    "primary_ssic_code",
    "primary_ssic_description",
]
