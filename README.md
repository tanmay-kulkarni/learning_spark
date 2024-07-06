# Databricks CLI
* databricks --help

## Configuration
* databricks configure --token

## List contents
* databricks fs ls

## Create folder 
* databricks fs mkdirs dbfs:/exam_prep

## Upload folder to dbfs
* databricks fs cp retail_db dbfs:/exam_prep/retail_db --recursive
* databricks fs cp retail_db_json dbfs:/exam_prep/retail_db_json --recursive