# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Load raw sources and lookups
# MAGIC Reads the source files and lookup files from the volume and stores them unchanged
# MAGIC (all columns as string) as Delta tables. These tables are the input for notebook 02.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from alm_etl import config
from alm_etl.io import read_source, write_table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw sources
# MAGIC Source1 (and the exchange rates lookup) has an unnamed index column, which Spark names `_c0`.
# MAGIC It is kept, because raw tables hold the data unchanged.

# COMMAND ----------

for source in config.SOURCES.values():
    df = read_source(spark, source["path"], source["format"], source["options"])
    write_table(df, config.table_name(source["raw_table"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lookups

# COMMAND ----------

for lookup in config.LOOKUPS.values():
    df = read_source(spark, lookup["path"], lookup["format"], lookup["options"])
    write_table(df, config.table_name(lookup["table"]))
