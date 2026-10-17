"""Central configuration: paths, table names and the column mapping from MappingList.xlsx.

Everything that comes from the business mapping lives here, so the transformation
functions stay generic and are driven by this config.
"""

CATALOG = "workspace"
SCHEMA = "alm"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"


def table_name(name: str) -> str:
    """Return the fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


# Raw sources. Every column is read as string so the raw tables hold the data unchanged.
# column_mapping: source column -> final column (from the 'Mapping' tab).
SOURCES = {
    "Source1": {
        "path": f"{VOLUME_PATH}/Source Files/Source1.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "false"},
        "raw_table": "raw_source1",
        "column_mapping": {
            "DataSource": "SourceSystem",
            "ClientGroup": "ClientGroup",
            "ClientNumber": "ClientNumber",
            "ClientAmount": "OriginalAmount",
            "Currency": "OriginalCurrency",
            "NumberOfEmployees": "NumberOfEmployees",
            "ClientSince": "OnboardingDate",
            "Location": "Country",
            "EligibleForDiscount": "DiscountCheck",
            "SnapshotDate": "SnapshotDate",
        },
    },
    "Source2": {
        "path": f"{VOLUME_PATH}/Source Files/Source2",
        "format": "parquet",
        "options": {},
        "raw_table": "raw_source2",
        "column_mapping": {
            "SourceSystem": "SourceSystem",
            "ClientGroup": "ClientGroup",
            "ClientNumber": "ClientNumber",
            "ClientAmount": "OriginalAmount",
            "Currency": "OriginalCurrency",
            "NumberOfEmployees": "NumberOfEmployees",
            "ClientSince": "OnboardingDate",
            "HasLoan": "LoanCheck",
            "SnapshotDate": "SnapshotDate",
        },
    },
}

LOOKUPS = {
    "exchange_rates": {
        "path": f"{VOLUME_PATH}/Lookup Files/Exchange_Rates.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "false"},
        "table": "lookup_exchange_rates",
    },
    "client_secured_ind": {
        "path": f"{VOLUME_PATH}/Lookup Files/Client_Secured_Ind.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "false", "sep": ";"},
        "table": "lookup_client_secured_ind",
    },
}

# Final table: (column, Spark SQL type, default value used when source/lookup data is missing).
# Order defines the column order of the final table. None means null.
FINAL_SCHEMA = [
    ("SourceSystem", "string", None),
    ("ClientGroup", "string", None),
    ("ClientNumber", "int", None),
    ("OriginalAmount", "decimal(15,2)", None),
    ("OriginalCurrency", "string", None),
    ("NumberOfEmployees", "int", None),
    ("OnboardingDate", "date", None),
    ("LoanCheck", "string", "NA"),
    ("Country", "string", None),
    ("DiscountCheck", "string", "NA"),
    ("SnapshotDate", "date", None),
    ("AmountEUR", "decimal(15,2)", None),
    ("ClientSecuredIND", "boolean", None),
    ("EnterprizeSize", "string", "NA"),
    ("SecuredAmountEUR", "decimal(15,2)", None),
]

# Columns produced by the mapping step (everything except the derived columns).
DERIVED_COLUMNS = ["AmountEUR", "ClientSecuredIND", "EnterprizeSize", "SecuredAmountEUR"]
MAPPED_COLUMNS = [col for col, _, _ in FINAL_SCHEMA if col not in DERIVED_COLUMNS]

YES_NO_COLUMNS = ["LoanCheck", "DiscountCheck"]

BASE_CURRENCY = "EUR"
SECURED_AMOUNT_CAP_EUR = 500_000

FINAL_TABLE = "final_client_amounts"
FINAL_PARTITION_COLUMNS = ["SnapshotDate"]
