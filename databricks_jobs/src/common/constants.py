"""
Shared constants used across the data pipeline.
"""

# Storage paths
BRONZE_PATH = (
    "abfss://lakehouse@atliqcommerce.dfs.core.windows.net/bronze"
)

# Unity Catalog objects
CATALOG_NAME = "atliq"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
