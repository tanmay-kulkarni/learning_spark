# Steps to configure Databricks CLI

1. Install Databricks CLI

    * pip install databricks-cli

2. Configure Databricks CLI

    * databricks configure --token
    
        * Enter the URL of your Databricks workspace: https://<your-databricks-instance>

        * Enter token: <your-databricks-token> -- create this from your databricks account settings

3. Download the practice dataset

    * git clone https://github.com/dgadiraju/retail_db.git
    * remove the .git directory in the retail_db directory

4. Create necessary directories in DBFS

    * databricks fs mkdirs dbfs:/exam_prep



