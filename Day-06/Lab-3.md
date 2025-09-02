# Hands-On Lab: Building a Python DLT Pipeline

## Goal

This lab guides you through building a complete Delta Live Tables (DLT) pipeline using Python. You will ingest raw data, apply transformations and data quality rules, create aggregated tables, configure and run the pipeline, explore operational modes, and learn basic troubleshooting using event logs.

---

## Prerequisites

- Databricks workspace with DLT enabled
- Permissions to create clusters and DLT pipelines
- Sample CSV files: `customer_details.csv`, `product_catalog.csv`, `sales.csv`

---

## Lab Setup: Uploading Sample Data

Before building the DLT pipeline, you need to upload the provided CSV files to a location accessible by Databricks, typically DBFS (Databricks File System) or cloud storage. For this lab, we'll use DBFS.

**Navigate to DBFS:**
- In your Databricks workspace, go to **Data** in the left sidebar.
- Click on the **DBFS** tab.

**Create a Directory:**
- Navigate to a suitable location in DBFS, for example: `/FileStore/dlt_lab_data_batch/`.
- Create three subdirectories:
    - `raw_customer_details`
    - `raw_product_catalog`
    - `raw_sales`
- You can do this via the UI (Upload button, then create folder) or using Databricks CLI / `dbutils.fs.mkdirs()`.

**Upload Files:**
- Upload `customer_details.csv` to `/FileStore/dlt_lab_data_batch/raw_customer_details/customer_details.csv`.
- Upload `product_catalog.csv` to `/FileStore/dlt_lab_data_batch/raw_product_catalog/product_catalog.csv`.
- Upload `sales.csv` to `/FileStore/dlt_lab_data_batch/raw_sales/sales.csv`.

> **Important:** For batch reads, you'll point to the specific file or the directory containing the files.

> **Note:** Replace `/FileStore/dlt_lab_data_batch/` with your chosen DBFS path throughout the lab if you use a different location.

---

## Part 1: Creating the DLT Python Notebook

**Create a New Notebook:**
1. In your Databricks workspace, go to **Workspace > Users > Your Username**.
2. Click the down arrow next to your name and select **Create > Notebook**.
3. Name your notebook (e.g., `dlt_retail_pipeline_python_batch`).
4. Set the Default Language to **Python**.
5. Ensure it's attached to a running cluster for initial syntax checking (though DLT will use its own cluster).

**Import DLT and PySpark Functions:**  
Add the following to the first cell of your notebook:

```python
import dlt
from pyspark.sql.functions import col, expr, to_date, year, current_date, upper, trim, regexp_replace, when, lit, sha2, concat_ws, lower
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
```

---

## Part 2: Defining Bronze Layer Tables (Batch Ingestion)

The Bronze layer ingests raw data from source systems. In this version, we'll use standard Spark batch reads for the CSV files.

### 2.1 Bronze Customer Details

Add the following code to a new cell:

```python
# --- Bronze Customer Details (Batch Read) ---
@dlt.table(
        name="bronze_customer_details_batch",
        comment="Raw customer details ingested from CSV file using batch read.",
        table_properties={
                "quality": "bronze",
                "data_source": "customer_details_csv_batch"
        }
)
def bronze_customer_details_batch():
        schema = "customer_id STRING, first_name STRING, last_name STRING, email STRING, date_of_birth STRING, address STRING, country_code STRING"
        return (
                spark.read
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load("/FileStore/dlt_lab_data_batch/raw_customer_details/customer_details.csv")
                .withColumn("input_file_name", lit("/FileStore/dlt_lab_data_batch/raw_customer_details/customer_details.csv"))
                .withColumn("processed_timestamp", expr("current_timestamp()"))
        )
```

**Explanation:**
- `@dlt.table`: Decorator to define a DLT table.
- `spark.read.format("csv")`: Reads data as a batch DataFrame.
- `.schema(schema)`: Explicitly defining the schema is best practice for CSVs to avoid type issues and ensure consistency.
- `.option("header", "true")`: Indicates the CSV has a header row.
- `.load()`: Specifies the path to the raw data file(s).
- `lit(...)`: For batch reads, if you need the input file name, you often have to add it literally or derive it if reading multiple files from a directory using other Spark APIs. For simplicity with a single file, we use `lit`.

---

### 2.2 Bronze Product Catalog

Add the following code to a new cell:

```python
# --- Bronze Product Catalog (Batch Read) ---
@dlt.table(
    name="bronze_product_catalog_batch",
    comment="Raw product catalog data ingested from CSV file using batch read.",
    table_properties={
        "quality": "bronze",
        "data_source": "product_catalog_csv"
    }
)
def bronze_product_catalog_batch():
    # Schema based on your provided column names for product_catalog.csv
    # Reading UnitPrice as DOUBLE directly. Spark will turn unparseable values into null.
    schema = "ProductID STRING, Item STRING, Category STRING, UnitPrice DOUBLE"
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load("/FileStore/dlt_lab_data/raw_product_catalog/product_catalog.csv")
        .withColumn("input_file_name", F.lit("/FileStore/dlt_lab_data/raw_product_catalog/product_catalog.csv"))
        .withColumn("processed_timestamp", F.expr("current_timestamp()"))
    )
```

---

### 2.3 Bronze Sales Data

Add the following code to a new cell:

```python
# --- Bronze Sales Data (Batch Read) ---
@dlt.table(
    name="bronze_sales_batch",
    comment="Raw sales transaction data ingested from CSV file using batch read.",
    table_properties={
        "quality": "bronze",
        "data_source": "sales_csv" # 
    }
)
def bronze_sales_batch():
    # Schema based on your provided column names for sales.csv
    # Reading numeric fields (Quantity, UnitPrice, TaxAmount) as their target types directly.
    schema = """
        SalesOrderNumber STRING, SalesOrderLineNumber STRING, OrderDate STRING, 
        CustomerID STRING, Item STRING, 
        Quantity INTEGER, UnitPrice DOUBLE, TaxAmount DOUBLE
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema) 
        .load("/FileStore/dlt_lab_data/raw_sales/sales.csv")
        .withColumn("input_file_name", F.lit("/FileStore/dlt_lab_data/raw_sales/sales.csv"))
        .withColumn("processed_timestamp", F.expr("current_timestamp()"))
    )

```

---

## Part 3: Defining Silver Layer Tables

The Silver layer refines Bronze data. Since our Bronze tables are now based on batch reads, we'll use `dlt.read()` to process them in the Silver layer. DLT will manage the dependencies and updates.

### 3.1 Silver Customer Details

Add the following code to a new cell:

```python
# --- Silver Customer Details ---
@dlt.table(
    name="silver_customer_details_batch",
    comment="Cleaned and conformed customer details with data quality checks (from batch source).",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("customer_id_not_null", "CustomerID IS NOT NULL") 
@dlt.expect("valid_email_format", "EmailAddress RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}' OR EmailAddress IS NULL") 
def silver_customer_details_batch():
    bronze_customers_df = dlt.read("bronze_customer_details_batch")
    
    customers_whitespace_trimmed_df = bronze_customers_df.withColumn(
        "CustomerID_whitespace_trimmed", 
        F.trim(F.col("CustomerID")) 
    )
    
    customers_final_id_df = customers_whitespace_trimmed_df.withColumn(
        "CustomerID_cleaned",
        F.when(
            F.col("CustomerID_whitespace_trimmed").isNotNull(),
            F.split(F.col("CustomerID_whitespace_trimmed"), "_")[0] 
        ).otherwise(F.col("CustomerID_whitespace_trimmed")) 
    )

    return (
        customers_final_id_df
        .withColumn("EmailAddress_cleaned", F.lower(F.trim(F.col("EmailAddress")))) 
        .withColumn("Country_cleaned", F.upper(F.trim(F.col("Country"))))          
        .withColumn("customer_sk", F.sha2(F.concat_ws("||", F.col("CustomerID_cleaned")), 256)) 
        .select(
            F.col("customer_sk"),
            F.col("CustomerID_cleaned").alias("CustomerID"), 
            F.col("CustomerName"),
            F.col("EmailAddress_cleaned").alias("EmailAddress"),
            F.col("PhoneNumber"), 
            F.col("Address"),    
            F.col("City"),        
            F.col("State"),       
            F.col("PostalCode"),  
            F.col("Country_cleaned").alias("Country"),
            F.col("input_file_name"),
            F.col("processed_timestamp")
        )
    )

```

**Explanation:**
- `dlt.read("bronze_customer_details_batch")`: Reads from the upstream Bronze table. Since the Bronze table is defined from a batch operation, `dlt.read()` is appropriate here. DLT understands this dependency.

---

### 3.2 Silver Product Catalog

Add the following code to a new cell:

```python
# --- Silver Product Catalog ---
@dlt.table(
    name="silver_product_catalog_batch",
    comment="Cleaned and conformed product catalog with data quality checks (from batch source).",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("product_id_not_null", "ProductID IS NOT NULL") 
@dlt.expect("valid_item_name", "Item IS NOT NULL") 
@dlt.expect("valid_unit_price_catalog", "UnitPrice > 0 OR UnitPrice IS NULL") 
def silver_product_catalog_batch():
    bronze_products_df = dlt.read("bronze_product_catalog_batch")
    return (
        bronze_products_df
        .withColumn("ProductID_cleaned", F.trim(F.col("ProductID")))
        .withColumn("Item_cleaned", F.trim(F.col("Item")))
        .withColumn("Category_cleaned", F.trim(F.upper(F.col("Category"))))
        .select(
            F.col("ProductID_cleaned").alias("ProductID"),
            F.col("Item_cleaned").alias("Item"),
            F.col("Category_cleaned").alias("Category"),
            F.col("UnitPrice"), 
            F.col("input_file_name"),
            F.col("processed_timestamp")
        )
    )

```

---

### 3.3 Silver Sales Data

Add the following code to a new cell:

```python
# --- Silver Sales Data ---
@dlt.table(
    name="silver_sales_batch",
    comment="Cleaned and conformed sales transactions with data quality checks (from batch source).",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("sales_order_number_not_null", "SalesOrderNumber IS NOT NULL") 
@dlt.expect_or_drop("customer_id_not_null_sales", "CustomerID IS NOT NULL")       
@dlt.expect_or_drop("item_not_null_sales", "Item IS NOT NULL")                   
@dlt.expect("positive_quantity", "Quantity > 0 OR Quantity IS NULL") 
@dlt.expect("valid_unit_price_sales", "UnitPrice > 0 OR UnitPrice IS NULL") 
def silver_sales_batch():
    bronze_sales_df = dlt.read("bronze_sales_batch") 
    
    sales_whitespace_trimmed_df = bronze_sales_df.withColumn(
        "CustomerID_whitespace_trimmed",
        F.trim(F.col("CustomerID")) 
    )
    sales_final_id_df = sales_whitespace_trimmed_df.withColumn(
        "CustomerID_cleaned", 
        F.when(
            F.col("CustomerId_whitespace_trimmed").isNotNull(),
            F.split(F.col("CustomerId_whitespace_trimmed"), "_")[0]
        ).otherwise(F.col("CustomerId_whitespace_trimmed"))
    )

    return (
        sales_final_id_df 
        .withColumn("SalesOrderNumber_cleaned", F.trim(F.col("SalesOrderNumber")))
        .withColumn("Item_cleaned", F.trim(F.col("Item"))) 
        .withColumn("OrderDate_dt", F.to_date(F.col("OrderDate"), "MM/dd/yyyy"))
        .withColumn("TotalSaleAmount", F.expr("Quantity * UnitPrice + TaxAmount")) 
        .select(
            F.col("SalesOrderNumber_cleaned").alias("SalesOrderNumber"),
            F.col("SalesOrderLineNumber"),
            F.col("OrderDate_dt").alias("OrderDate"),
            F.col("CustomerID_cleaned").alias("CustomerID"), 
            F.col("Item_cleaned").alias("Item"),      
            F.col("Quantity"), 
            F.col("UnitPrice"), 
            F.col("TaxAmount"),
            F.col("TotalSaleAmount"),
            F.col("input_file_name"),
            F.col("processed_timestamp")
        )
    )

```

---

## Part 4: Defining Gold Layer Table

The Gold layer provides aggregated data. This remains unchanged as it already uses `dlt.read()`.

```python
# --- Gold Customer Sales Summary ---
@dlt.table(
    name="gold_customer_sales_summary_batch",
    comment="Aggregated sales summary per customer and product category (from batch sources).",
    table_properties={"quality": "gold"}
)
def gold_customer_sales_summary_batch():
    sales_df = dlt.read("silver_sales_batch") 
    customers_df = dlt.read("silver_customer_details_batch") 
    products_df = dlt.read("silver_product_catalog_batch")

    joined_df = sales_df.join(customers_df, sales_df.CustomerID == customers_df.CustomerID, "inner") \
                        .join(products_df, sales_df.Item == products_df.Item, "inner")

    return (
        joined_df
        .groupBy(
            customers_df.customer_sk, 
            customers_df.CustomerID, 
            customers_df.CustomerName, 
            customers_df.EmailAddress, 
            customers_df.Country,      
            products_df.Category       
        )
        .agg(
            F.sum("TotalSaleAmount").alias("total_revenue_inc_tax"), 
            F.countDistinct("SalesOrderNumber").alias("total_unique_orders"), 
            F.avg("TotalSaleAmount").alias("average_order_value_inc_tax"),  
            F.sum("Quantity").alias("total_quantity_sold") 
        )
    )

```

---

## Part 5: Configuring and Running the DLT Pipeline

The steps for configuring and running the DLT pipeline in the UI are largely the same.

1. **Go to Delta Live Tables UI:**
     - In your Databricks workspace, click **Workflows** in the left sidebar, then select the **Delta Live Tables** tab.

2. **Create Pipeline:**
     - Click **Create Pipeline**.

3. **Configure Pipeline Settings:**
     - **Pipeline Name:** Retail Sales Pipeline Python Batch Lab
     - **Pipeline Mode:** Select **Triggered**. (Continuous mode is less relevant for purely batch ingestion unless you plan to re-trigger based on new file arrivals manually or via an external scheduler).
     - **Notebook Libraries:** Select the Python DLT notebook you created (e.g., `dlt_retail_pipeline_python_batch`).
     - **Storage Location (Optional but Recommended):** E.g., `/delta_live_tables/retail_lab_python_batch`.
     - **Target Schema (Optional but Recommended):** E.g., `dlt_retail_lab_db_python_batch`.
     - **Cluster Policy, Cluster Mode, Workers, Photon, Channel, Notifications:** Configure as in the previous lab version. For batch, the cluster might not need to be as robust unless the batch processing itself is very heavy.
     - **Configuration (Advanced):** No specific Spark conf needed for this batch setup unless you encounter memory issues with large CSVs.

4. **Save and Start:**
     - Click **Create**, then **Start**.

5. **Monitor Pipeline Run:**
     - Observe the pipeline graph.

6. **Verify Data:**
     - Once completed, query your tables in the target schema (e.g., `dlt_retail_lab_db_python_batch`).

```sql
SELECT CustomerID, length(CustomerID) as id_length FROM uc01.retail_sales.silver_customer_details_batch LIMIT 10;
SELECT CustomerID, length(CustomerID) as id_length FROM uc01.retail_sales.silver_sales_batch LIMIT 10;
SELECT CustomerID, customer_sk, CustomerName, total_revenue_inc_tax FROM uc01.retail_sales.gold_customer_sales_summary_batch ORDER BY total_revenue_inc_tax DESC LIMIT 10;
```

---

## Part 6: Exploring Pipeline Modes and Lineage

This section remains conceptually the same.

### 6.1 Pipeline Modes

- **Development vs. Production Mode** 
- **Triggered vs. Continuous Execution:**
    - With batch ingestion, **Triggered** mode is the natural fit. Each run processes the source files as they are.
    - Continuous mode with these batch `spark.read` sources would mean the pipeline runs once and then idles, as `spark.read` doesn't continuously monitor for new files like Auto Loader. To reprocess, you'd typically stop and restart a triggered pipeline, or rely on DLT's ability to pick up changes if the underlying data in the CSV path changes between triggered runs (though DLT's primary strength for incremental processing is with streaming sources or cloudFiles).

### 6.2 Data Lineage

- The DLT pipeline graph will still show the lineage correctly based on the `dlt.read()` dependencies.

---

