# Databricks notebook source
# MAGIC %run "./07_create_a_df"

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### all of the following expressions are valid for comparing true/false

# COMMAND ----------

df.where("is_customer = true").show()

# COMMAND ----------

df.where("is_customer = True").show()

# COMMAND ----------

df.filter("is_customer = 'true'").show()

# COMMAND ----------

df.filter("is_customer = 'True'").show()

# COMMAND ----------

df.filter("is_customer == 'True'").show()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col('is_customer') == True).show()

# COMMAND ----------

df.filter("is_customer == 'false'").show()
