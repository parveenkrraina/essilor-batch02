# Building SQL Delta Live Tables Pipelines with Expectations

**Duration:** ~60 minutes  
**Prerequisites:**  
- Access to Databricks workspace with Delta Live Tables enabled  
- Sample dataset uploaded (e.g., raw sales data CSV)

---

## Lab Objectives

- Create a Delta Live Tables pipeline using SQL declarative syntax
- Define Bronze, Silver, and Gold tables implementing the Medallion architecture
- Configure and apply data quality expectations in SQL pipelines
- Run and monitor the pipeline in the Delta Live Tables UI
- Observe pipeline logs and alerts triggered by expectation failures

---

## Step 1: Prepare Sample Data

- Upload or ensure sample raw data is available in a storage location accessible by Databricks (e.g., DBFS or cloud storage).
- **Example dataset:** `sales_raw.csv` with columns: `order_id`, `customer_id`, `order_date`, `item`, `quantity`, `unit_price`, `tax_amount`.

---

## Step 2: Create a New SQL Pipeline in Delta Live Tables UI

1. Go to the **Jobs and Pipeline** section in your Databricks workspace.
2. Click **Create ETL pipeline**.
3. Enter pipeline name, e.g., `medallion_sql_pipeline`.
4. Select appropriate cluster configuration (default is fine).
5. Set pipeline mode to **Trigger**.
6. Save the pipeline (don’t start it yet).
7. Set Destination Storage to Unity Catalog
8. Default Catalog: catalog01 -- Use this catalog for all tables as per your setup
9. Default Schema: sales -- Use this schema for all tables as per your setup
10. Cluster Mode Fixed Size
11. Workers 1
12. Target Schema: same as mentioned in the code above e.g. sales
13. Instance types Standard_DS3_v2 (Recommended for balanced performance and cost for typical ETL workloads)
14. Set Driver Type to Standard_DS3_v2 (same as worker node instance type)
15. From the source Code under the Pipeline details click on the Source code link.

---

## Step 3: Write SQL Pipeline Script

Create the pipeline script defining the Bronze, Silver, and Gold tables with expectations. Change the default language to SQL.

```sql
-- Bronze layer: raw ingestion (ingest all raw data as-is)
CREATE OR REFRESH MATERIALIZED VIEW bronze_sales_raw
AS
SELECT *
FROM read_files(
  "/FileStore/DLT/sales.csv",
  format => "csv",
  header => "true",
  inferSchema => "true"
);

-- Silver layer: clean and deduplicate, add data quality expectations
CREATE OR REFRESH MATERIALIZED VIEW silver_sales_cleaned (
  SalesOrderNumber STRING,
  SalesOrderLineNumber INT, 
  OrderDate DATE,
  CustomerId STRING,
  Item STRING,
  Quantity INT,
  UnitPrice DOUBLE,
  TaxAmount DOUBLE,
  
  -- Data quality expectations with violation handling
  CONSTRAINT order_id_not_null EXPECT (SalesOrderNumber IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT order_date_not_null EXPECT (OrderDate IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT quantity_valid EXPECT (Quantity BETWEEN 1 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT price_positive EXPECT (UnitPrice > 0) ON VIOLATION DROP ROW,
  CONSTRAINT tax_non_negative EXPECT (TaxAmount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT order_date_valid EXPECT (OrderDate <= CURRENT_DATE()) ON VIOLATION DROP ROW
)
AS
SELECT
  SalesOrderNumber,
  CAST(SalesOrderLineNumber AS INT) AS SalesOrderLineNumber,  -- Explicit cast to ensure type match
  to_date(OrderDate, 'dd-MM-yyyy') AS OrderDate,  -- Adjust format if dates differ
  CustomerId,
  Item,
  CAST(Quantity AS INT) AS Quantity,
  CAST(UnitPrice AS DOUBLE) AS UnitPrice,
  CAST(TaxAmount AS DOUBLE) AS TaxAmount
FROM LIVE.bronze_sales_raw;

-- Gold layer: aggregated sales data for reporting
CREATE OR REFRESH MATERIALIZED VIEW gold_sales_aggregated
AS
SELECT
  CustomerId,
  DATE_TRUNC('month', OrderDate) AS SalesMonth,
  COUNT(DISTINCT SalesOrderNumber) AS NumberOfOrders,
  SUM(Quantity) AS TotalQuantity,
  SUM(Quantity * UnitPrice) AS TotalSales,
  SUM(TaxAmount) AS TotalTax
FROM LIVE.silver_sales_cleaned
GROUP BY CustomerId, DATE_TRUNC('month', OrderDate);

```

- Paste this SQL script into the pipeline editor in the UI.
- Save the script.

---

## Step 4: Start and Run the Pipeline
- Navigate back to your pipeline created in the earlier steps.
- Click **Start** to run the pipeline.
- Monitor pipeline progress in the UI dashboard.
- Wait until the pipeline run completes successfully.

---

## Step 5: Review Pipeline Results and Quality Checks

- Navigate to the **Expectations** tab of the pipeline run.
- Confirm that all expectations passed.
- Intentionally modify the raw data (e.g., introduce duplicate or null `order_id`) and rerun the pipeline.
- Observe how failing expectations trigger alerts or pipeline errors.
- Check logs to understand the cause of failure.

---

## Step 6: Query Result Tables

In a Databricks notebook or SQL editor, run queries to validate output:

```sql
-- Bronze raw data
SELECT * FROM catalog01.sales.bronze_sales_raw LIMIT 10;

-- Silver cleaned data
SELECT * FROM catalog01.sales.silver_sales_cleaned LIMIT 10;

-- Gold aggregated data
SELECT * FROM catalog01.sales.gold_sales_aggregated ORDER BY order_date DESC LIMIT 10;
```
