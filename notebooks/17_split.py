# Databricks notebook source
# MAGIC %run "./07_create_a_df"

# COMMAND ----------

from pyspark.sql.functions import split, explode, concat, col, lit

# COMMAND ----------

help(split)

# COMMAND ----------

concated_courses = concat(
    col("courses")[0], lit(","), col("courses")[1], lit(","), col("courses")[2]
).alias("courses_str")

df.select(
    "id",
    "email",
    explode(split(concated_courses, ",").alias("split_courses")).alias("subject"),
).show(truncate=False)
