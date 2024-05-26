# Databricks notebook source
# MAGIC %run "./07_create_a_df"

# COMMAND ----------

columns_to_select = ['id', 'first_name', 'is_customer', 'amount_paid']
df.select(columns_to_select).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ### filtering using col

# COMMAND ----------

df.filter(col('amount_paid') > 50).select(columns_to_select).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### can also use SQL style syntax

# COMMAND ----------

df.where("amount_paid between 50 and 100").select(columns_to_select).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### where is an alias for filter

# COMMAND ----------

df.where("amount_paid > 50").select(columns_to_select).show()
