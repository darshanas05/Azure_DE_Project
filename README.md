# Azure_DE_Project

**Tech Stack:**

**Azure Blob Storage Azure Data Lake Gen 2:** Source for incoming CSV files and destination for processed files.

**Azure Data Factory:** Orchestration tool for managing the data pipeline.

**Databricks:** Used for data processing and quality checks.

**Azure Key Vault:** Securely stores secrets and credentials.


****Process Overview:****

**Azure Blob/Azure Data Lake Setup:**
Configure the source Blob storage for incoming files and create separate Blob storage containers for staging and rejected files.

**Databricks Setup:**
Create a Databricks workbook and setup a cluster. Write PySpark code for data quality checks and processing.

**PySpark Code:**
Read the CSV file, perform data quality checks (e.g., checking for duplicates or NULL values), and set error flags accordingly.

**Azure Key Vault:**
Store secrets like SAS tokens for Blob storage and other credentials securely.

**Azure Data Factory Setup:**
Create an ADF account, configure linked services for Blob storage, Databricks, and Key Vault. Build a pipeline to orchestrate the data processing flow.

**Pipeline Execution:**
Trigger the pipeline when a new file is added to the source folder. The pipeline reads the file name from the trigger and executes the Databricks notebook for processing.

**File Handling:** 
Based on the outcome of data quality checks, move the processed file to either the staging or rejected folder using dbutils in Databricks.

This setup ensures a systematic flow of data processing while maintaining security and scalability using Azure services.




