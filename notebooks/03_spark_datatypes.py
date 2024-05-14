# Databricks notebook source
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
Ï
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
