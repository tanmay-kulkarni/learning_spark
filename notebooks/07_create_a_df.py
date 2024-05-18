# Databricks notebook source
from pyspark.sql import Row
import datetime

# COMMAND ----------

from pyspark.sql import Row
import datetime

users = [
    {
        "id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "phone_numbers": Row(mobile=1234567890, home=9876543210),
        "courses": ["Math", "Science", "History"],
        "is_customer": True,
        "amount_paid": 100.50,
        "customer_from": datetime.date(2022, 5, 15),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 2,
        "first_name": "Jane",
        "last_name": "Smith",
        "email": "jane.smith@example.com",
        "phone_numbers": Row(mobile=9876543210, home=1234567890),
        "courses": ["English", "Art", "Music"],
        "is_customer": False,
        "amount_paid": 0.0,
        "customer_from": datetime.date(2023, 2, 10),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 3,
        "first_name": "Alice",
        "last_name": "Johnson",
        "email": "alice.johnson@example.com",
        "phone_numbers": Row(mobile=5555555555, home=4444444444),
        "courses": ["Physics", "Chemistry", "Biology"],
        "is_customer": True,
        "amount_paid": 75.20,
        "customer_from": datetime.date(2021, 8, 20),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 4,
        "first_name": "Robert",
        "last_name": "Brown",
        "email": "robert.brown@example.com",
        "phone_numbers": Row(mobile=7777777777, home=8888888888),
        "courses": ["Computer Science", "Programming", "Data Structures"],
        "is_customer": True,
        "amount_paid": 50.0,
        "customer_from": datetime.date(2023, 1, 5),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 5,
        "first_name": "Emily",
        "last_name": "Lee",
        "email": "emily.lee@example.com",
        "phone_numbers": Row(mobile=9999999999, home=1111111111),
        "courses": ["Literature", "Writing", "Poetry"],
        "is_customer": True,
        "amount_paid": 30.75,
        "customer_from": datetime.date(2023, 4, 12),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 6,
        "first_name": "Michael",
        "last_name": "Johnson",
        "email": "michael.johnson@example.com",
        "phone_numbers": Row(mobile=5555555555, home=4444444444),
        "courses": ["Physics", "Chemistry", "Biology"],
        "is_customer": True,
        "amount_paid": 75.20,
        "customer_from": datetime.date(2021, 8, 20),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 7,
        "first_name": "Sarah",
        "last_name": "Davis",
        "email": "sarah.davis@example.com",
        "phone_numbers": Row(mobile=7777777777, home=8888888888),
        "courses": ["Computer Science", "Programming", "Data Structures"],
        "is_customer": True,
        "amount_paid": 50.0,
        "customer_from": datetime.date(2023, 1, 5),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 8,
        "first_name": "Jessica",
        "last_name": "Wilson",
        "email": "jessica.wilson@example.com",
        "phone_numbers": Row(mobile=9999999999, home=1111111111),
        "courses": ["Literature", "Writing", "Poetry"],
        "is_customer": True,
        "amount_paid": 30.75,
        "customer_from": datetime.date(2023, 4, 12),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 9,
        "first_name": "David",
        "last_name": "Anderson",
        "email": "david.anderson@example.com",
        "phone_numbers": Row(mobile=5555555555, home=4444444444),
        "courses": ["Physics", "Chemistry", "Biology"],
        "is_customer": True,
        "amount_paid": 75.20,
        "customer_from": datetime.date(2021, 8, 20),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
    {
        "id": 10,
        "first_name": "Jennifer",
        "last_name": "Taylor",
        "email": "jennifer.taylor@example.com",
        "phone_numbers": Row(mobile=7777777777, home=8888888888),
        "courses": ["Computer Science", "Programming", "Data Structures"],
        "is_customer": True,
        "amount_paid": 50.0,
        "customer_from": datetime.date(2023, 1, 5),
        "last_updated_ts": datetime.datetime(2024, 5, 18, 11, 28, 25),
    },
]

# COMMAND ----------

df = spark.createDataFrame([Row(**user) for user in users])
df.dtypes

# COMMAND ----------

df.show(truncate=False)
