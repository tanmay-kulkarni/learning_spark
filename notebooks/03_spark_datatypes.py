# Databricks notebook source
# MAGIC %md
# MAGIC ### Specify types with schema as a string

# COMMAND ----------

import datetime

users = [
    (
        1,
        "Tanmay",
        "Kulkarni",
        True,
        datetime.date(2024, 1, 30),
        datetime.datetime(2023, 3, 10, 11, 16, 56),
    ),
    (
        2,
        "Tyrion",
        "Lannister",
        False,
        None,
        datetime.datetime(2023, 3, 10, 11, 16, 56),
    ),
]

schema = """
    id INT,
    first_name STRING,
    last_name STRING,
    is_customer BOOLEAN,
    date_of_joining DATE,
    last_updated TIMESTAMP
"""

df = spark.createDataFrame(users, schema)

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Specify types using Spark types

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
)

fields = StructType(
    [
        StructField("id", IntegerType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("is_customer", BooleanType()),
        StructField("date_of_joining", DateType()),
        StructField("last_updated", TimestampType()),
    ]
)


df = spark.createDataFrame(users, fields)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert a spark dataframe to a pandas dataframe

# COMMAND ----------

pandas_df = df.toPandas()
pandas_df.head()
