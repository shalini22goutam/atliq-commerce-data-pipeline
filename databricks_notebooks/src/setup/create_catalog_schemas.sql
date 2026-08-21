CREATE CATALOG IF NOT EXISTS atliq
MANAGED LOCATION 'abfss://lakehouse@atliqcommerce.dfs.core.windows.net/atliq-managed';

CREATE SCHEMA IF NOT EXISTS atliq.silver;

CREATE SCHEMA IF NOT EXISTS atliq.gold;

CREATE SCHEMA IF NOT EXISTS atliq.ci;