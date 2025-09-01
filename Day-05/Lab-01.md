# Databricks Unity Catalog: End-to-End Data Governance & Security Lab
**This notebook provides a step-by-step lab for data governance and security using Unity Catalog in Databricks.**

**Key tasks:**
- Catalog/schema/table setup
- Data migration
- Encryption awareness
- Access control (including dynamic views)
- Data lifecycle management
- Marketplace integration
- Audit and validation

---
## Prerequisites
- Unity Catalog must be enabled.
- You need sufficient permissions (admin or equivalent for governance tasks).
- Sample data should be available in DBFS or legacy databases.
---

```python
%sql
CREATE SCHEMA IF NOT EXISTS spark_catalog.legacy_db;

```

```python
# sales.csv
df_sales = spark.read.csv('/FileStore/data/sales.csv', header=True, inferSchema=True)
df_sales.write.mode('overwrite').saveAsTable('spark_catalog.legacy_db.sales')

# customer_details.csv
df_cust = spark.read.csv('/FileStore/data/customer_details.csv', header=True, inferSchema=True)
df_cust.write.mode('overwrite').saveAsTable('spark_catalog.legacy_db.customer_details')

# product_catalog.csv
df_prod = spark.read.csv('/FileStore/data/product_catalog.csv', header=True, inferSchema=True)
df_prod.write.mode('overwrite').saveAsTable('spark_catalog.legacy_db.product_catalog')

```

```python
%sql
SELECT * FROM spark_catalog.legacy_db.sales LIMIT 5;

```

## 1. Create Catalog, Schema, and Table
**Why:** Establishes data organization and logical boundaries for governance.

**Action:** Create a catalog, then a schema, then a managed table (example uses DELTA format).

**Note:** You can omit the location parameter in this lab, as the catalog already has a managed location assigned.

**Explanation:** This establishes a new catalog for organizing data assets and applying governance policies.

```python
%sql
CREATE CATALOG IF NOT EXISTS uc_demo
  MANAGED LOCATION 'abfss://data@deassociateadls.dfs.core.windows.net/uc/uc_demo'
  COMMENT 'UC Demo Catalog for governance lab';

```

```python
%sql
CREATE SCHEMA IF NOT EXISTS uc_demo.raw;

```

## 2. Migrate Existing Table/Data to Unity Catalog
**Why:** Bring legacy tables under Unity Catalog for unified governance and security.

**Action:** List legacy tables, then use CTAS (Create Table As Select) to migrate.

**Explanation:** This process involves creating new tables in the Unity Catalog and populating them with data from the legacy tables, ensuring a smooth transition to the new governance model.

```python
%sql
CREATE TABLE IF NOT EXISTS uc_demo.raw.sales AS
SELECT * FROM spark_catalog.legacy_db.sales;

CREATE TABLE IF NOT EXISTS uc_demo.raw.customer_details AS
SELECT * FROM spark_catalog.legacy_db.customer_details;

CREATE TABLE IF NOT EXISTS uc_demo.raw.product_catalog AS
SELECT * FROM spark_catalog.legacy_db.product_catalog;

```

```python
%sql
SELECT * FROM uc_demo.raw.sales LIMIT 5;

```
Show the list of databases in the hive_metastore
```python
%sql
SHOW DATABASES;
```
Show the list of databases in the Unity Catalog - change the name as per you catalog name
```python
%sql
SHOW DATABASES IN uc_demo;
```


```python
# Optional: List legacy tables
display(spark.sql('SHOW TABLES IN spark_catalog.default'))
```

## 3. Encryption Awareness
**Why:** All data in Unity Catalog is encrypted at rest and in transit. Customer-managed keys (CMK) are configured by admins at the workspace/cloud level.**

**Action:** No code required here for most users; verify via Admin Console if needed.

**Explanation:** This ensures that all data is encrypted and meets compliance requirements.

## 4. Apply Security Policies & Fine-Grained Access Control (Needs additional permission to access Entra that is not possible in current environment.)
**Why:** Ensures only authorized users/groups can access or manipulate data, fulfilling compliance and privacy requirements.**

**Action:** Grant usage, grant table SELECT, and build a dynamic view for row-level security.

**Note:** Required Groups have been already added. Please check the permissions. Change your catalog name and schema name as per your setup.

**Explanation:** This ensures that only authorized users/groups can access or manipulate data, fulfilling compliance and privacy requirements.

```python
%sql
/* Grant catalog and schema usage to a group (replace `data_engineers` with your real group)*/
GRANT USAGE ON CATALOG uc_demo TO `data_engineers`;
GRANT USAGE ON SCHEMA uc_demo.raw TO `data_engineers`;

GRANT SELECT ON TABLE uc_demo.raw.sales TO `analysts`;
GRANT SELECT ON TABLE uc_demo.raw.customer_details TO `analysts`;
GRANT SELECT ON TABLE uc_demo.raw.product_catalog TO `analysts`;

```

```python
%sql
CREATE OR REPLACE VIEW uc_demo.raw.customer_details_masked AS
SELECT
  *,
  CASE WHEN is_member('admins') THEN EmailAddress ELSE '***MASKED***' END AS masked_email
FROM uc_demo.raw.customer_details;

GRANT SELECT ON VIEW uc_demo.raw.customer_details_masked TO `account users`;

```

## 5. Data Lifecycle Management
**Why:** Automates data retention and cleanup for compliance and cost control.**

**Action:** Set a Delta retention policy and use VACUUM to clean up old files.

**Explanation:** This ensures that deleted files are retained for a specific duration, allowing for potential recovery, while also enabling the cleanup of files that are no longer needed.

```python
%sql
ALTER TABLE uc_demo.raw.sales SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

VACUUM uc_demo.raw.sales RETAIN 720 HOURS;

```

## 6. Audit Logs & Validation
**Why:** Enables compliance monitoring, troubleshooting, and validation of security settings.**

**Action:** Audit logs are accessed via Admin Console or your cloud provider. Validate row-level security:

**Explanation:** This ensures that only authorized users can access sensitive data, fulfilling compliance and privacy requirements.

```python
# Query as analyst (or test with different users)
display(spark.sql('SELECT * FROM uc_demo.raw.sales LIMIT 5'))
```

## 7. Clean Up (Optional)
**Why:** Remove resources to save cost or reset lab for the next run.**

**Action:** Uncomment and run as needed.

```python
%sql
DROP VIEW IF EXISTS uc_demo.raw.customer_details_masked;
DROP TABLE IF EXISTS uc_demo.raw.sales;
DROP TABLE IF EXISTS uc_demo.raw.customer_details;
DROP TABLE IF EXISTS uc_demo.raw.product_catalog;
DROP SCHEMA IF EXISTS uc_demo.raw CASCADE;
DROP CATALOG IF EXISTS uc_demo CASCADE;
```

---
### End of Databricks Unity Catalog Governance Lab
- Review audit logs for compliance.
- Validate access as different users.
