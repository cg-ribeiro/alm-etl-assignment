# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Results
# MAGIC Answers to the assignment questions, queried from the final table `workspace.alm.final_client_amounts`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. SecuredAmountEUR for ClientNumber 6991

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ClientNumber, SnapshotDate, SecuredAmountEUR
# MAGIC FROM workspace.alm.final_client_amounts
# MAGIC WHERE ClientNumber = 6991

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Number of records with EnterprizeSize 'M'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS records_size_m
# MAGIC FROM workspace.alm.final_client_amounts
# MAGIC WHERE EnterprizeSize = 'M'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Total AmountEUR for SourceSystem 'Source2'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(AmountEUR) AS total_amount_eur_source2
# MAGIC FROM workspace.alm.final_client_amounts
# MAGIC WHERE SourceSystem = 'Source2'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Total OriginalAmount where ClientSecuredIND is false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(OriginalAmount) AS total_original_amount_not_secured
# MAGIC FROM workspace.alm.final_client_amounts
# MAGIC WHERE ClientSecuredIND = false
