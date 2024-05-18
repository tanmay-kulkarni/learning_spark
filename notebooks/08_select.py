# Databricks notebook source
# MAGIC %md
# MAGIC ### Create a df from notebook: 07_create_a_df

# COMMAND ----------

# MAGIC %run "./07_create_a_df"

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.select('*').show(truncate=False)

# COMMAND ----------

df.columns

# COMMAND ----------

df.select('id', 'email', 'is_customer').show()

# COMMAND ----------

cols_to_select = ['id', 'email', 'is_customer']
df.select(cols_to_select).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### alias a dataframe

# COMMAND ----------

df.alias('u').select('u.first_name', 'u.last_name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### you can use a combination of col and just names in the select operation

# COMMAND ----------

from pyspark.sql.functions import col

df.select(col('id'), 'first_name', 'last_name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### using functions in 'select'

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

df.select('id', col('is_customer'), concat('last_name', lit(', '), col('first_name')).alias('full_name')).show()
