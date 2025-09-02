# Build a Delta Live Tables Pipeline with Medallion Architecture

## Prerequisites

- Access to a Databricks workspace with Delta Live Tables enabled.
- Upload your sample data CSV file (`sales.csv`) to Databricks FileStore or cloud storage accessible by Databricks.

---

## Step 1: Upload Sample Data

1. In Databricks, go to **Catalog > DBFS**.
2. Click **Upload** and select `sales.csv`.
3. Upload to: `/FileStore/sales.csv`.

*Note: You can also upload via CLI or directly to cloud storage (Azure Blob, S3) and update paths accordingly.*

---

## Step 2: Create Notebooks for DLT Pipeline

### Notebook 1: Bronze and Silver Layers (Python)

Create a new Python notebook named `Medallion_Pipeline_Python_Bronze_Silver` and add:

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="bronze_sales",
  comment="Raw sales data ingested to Bronze layer",
  table_properties={
    "quality": "bronze"
  }
)
def bronze_sales():
    return (
        spark.read.option("header", "true")
                  .option("inferSchema", "true")
                  .csv("/FileStore/sales.csv")
    )

@dlt.table(
  name="silver_sales",
  comment="Cleaned and deduplicated Silver sales data"
)
@dlt.expect("valid_quantity", "Quantity > 0")
@dlt.expect_or_drop("no_nulls", "CustomerId IS NOT NULL")
def silver_sales():
    df = dlt.read("bronze_sales")
    return df.filter(col("Quantity") > 0).dropDuplicates(["SalesOrderNumber", "SalesOrderLineNumber"])

```

---

### Notebook 2: Gold Layer (SQL) (Set dafault Language to SQL)

Create a new SQL notebook named `Medallion_Pipeline_SQL_Gold` and add:

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_sales_summary (
  OrderDate DATE,
  total_sales DOUBLE,
  unique_customers BIGINT,
  CONSTRAINT valid_total_sales EXPECT (total_sales > 0)
)
COMMENT "Aggregated daily sales totals"
AS
SELECT
  OrderDate,
  SUM(Quantity * UnitPrice) AS total_sales,
  COUNT(DISTINCT CustomerId) AS unique_customers
FROM
  silver_sales
GROUP BY
  OrderDate

```

---

## Step 3: Create Delta Live Tables Pipeline

1. Go to **Jobs > Delta Live Tables** in Databricks.
2. Click **Create Pipeline**.
3. Configure:
     - **Name:** `MedallionArchitecturePipeline`
     - **Pipeline Mode:** Triggered (manual) or Continuous (streaming)
     - **Notebook Libraries:** Attach both notebooks above
     - **Destination - Storage options**  Unity Catalog
     - **Default Catalog:** `catalog01` -- Use this catalog for all tables as per your setup
     - **Default Schema:** `sales` -- Use this schema for all tables as per your setup
     - **Cluster Mode**  Fixed Size
     - **Workers** 1
     - **Target Schema:** same as mentioned in the code above e.g. sales
     - **Instance types** Standard_DS3_v2
     - **Driver Type** Same as worker
4. Click **Create**.

---

## Step 4: Run the Pipeline

- In the DLT pipeline UI, click **Start** or **Trigger run**.
- Monitor progress:
    - Tables: `bronze_sales`, `silver_sales`, `gold_sales_summary`
    - Data quality expectations
    - Logs for errors

---

## Step 5: Query and Verify Data

**Note** Change Catalog and schema as per your setup.

Run in Databricks SQL notebook or editor:

```sql
SELECT * FROM catalog01.sales.bronze_sales LIMIT 10;
SELECT * FROM catalog01.sales.silver_sales LIMIT 10;
SELECT * FROM catalog01.sales.gold_sales_summary LIMIT 10;
```

- Confirm raw data in Bronze, cleaned data in Silver, and aggregated metrics in Gold.

---

## Step 6: Monitor & Manage Pipeline

- Use the Delta Live Tables UI to:
    - Check pipeline health and status
    - Review expectation pass/fail counts
    - Explore execution logs
    - Restart or stop pipeline as needed

---

## Best Practices

- Use expectations to ensure data quality.
- Build and test each layer incrementally.
- Modularize complex transformations.
- Schedule runs based on data arrival.
- Monitor pipelines regularly.
