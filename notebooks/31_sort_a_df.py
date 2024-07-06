# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run "./07_create_a_df"

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1

# COMMAND ----------

df.sort(F.col("first_name").asc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

df.sort(["email", "first_name"], ascending = [0,1]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 3

# COMMAND ----------

df.sort(F.desc(F.col('amount_paid')), F.asc(F.col('first_name'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### orderBy = sort

# COMMAND ----------

df.orderBy(F.desc('amount_paid'), F.asc('first_name')).show(truncate=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort by value size

# COMMAND ----------

df.sort(F.size(F.col('courses')).desc()).show(truncate=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prioritized/custom sorting

# COMMAND ----------

custom_sort = F.when(F.col('first_name').startswith('J'), 1).otherwise(2)

df.sort(custom_sort).show()
