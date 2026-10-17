# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Build the final table
# MAGIC 1. Read the raw source tables and lookup tables written by notebook 01.
# MAGIC 2. Per source: rename columns (mapping), normalize Yes/No to Y/N/NA and cast to the final types.
# MAGIC 3. Union the sources and add the derived columns: AmountEUR, ClientSecuredIND, EnterprizeSize, SecuredAmountEUR.
# MAGIC 4. Write the final Delta table partitioned by SnapshotDate.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from alm_etl import config
from alm_etl.io import write_table
from alm_etl.pipeline import build_final

# COMMAND ----------

source_dfs = {
    name: spark.table(config.table_name(source["raw_table"]))
    for name, source in config.SOURCES.items()
}
exchange_rates_df = spark.table(config.table_name(config.LOOKUPS["exchange_rates"]["table"]))
client_secured_df = spark.table(config.table_name(config.LOOKUPS["client_secured_ind"]["table"]))

final_df = build_final(source_dfs, exchange_rates_df, client_secured_df)
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC Only the snapshot dates being loaded are replaced (`replaceWhere`), so earlier snapshots stay intact
# MAGIC and a rerun is idempotent. On the first run the table does not exist yet, so it is a plain overwrite.

# COMMAND ----------

final_table = config.table_name(config.FINAL_TABLE)
snapshot_dates = [row.SnapshotDate for row in final_df.select("SnapshotDate").distinct().collect()]
replace_where = (
    "SnapshotDate IN ({})".format(", ".join(f"DATE'{d}'" for d in snapshot_dates))
    if spark.catalog.tableExists(final_table)
    else None
)

write_table(
    final_df,
    final_table,
    partition_columns=config.FINAL_PARTITION_COLUMNS,
    replace_where=replace_where,
)
