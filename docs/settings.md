# Settings Documentation

## 1. Source File Setup 
The source file location must contain all required configuration files such as .env, app.yaml, and any other necessary files to ensure proper environment setup.

## 2. Secrets Management 
All sensitive information (e.g., PAT tokens, SCIM tokens, credentials) must be stored securely in a secret scope and should not be hardcoded in the source code or configuration files. 

## 3. Email Configuration 
Email-related configurations must be correctly defined in the .env file to enable successful email notifications.

## 4. Audit Log Table Availability 
- The audit log table schema must already exist in the specified catalog.
- The table path should be configured and maintained in the .env file.

## 5. SQL Warehouse Configuration 
The SQL Warehouse HTTP Path must be defined in the .env file to enable connectivity with Databricks SQL Warehouse.


