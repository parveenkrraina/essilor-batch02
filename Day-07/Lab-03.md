# Hands-On Lab: Advanced Workflow Orchestration with Real Data

## Lab Scenario

Automate a data pipeline that:

- Ingests sales data and customer demographics.
- Cleans and transforms each dataset.
- Merges both datasets for analysis.
- Generates a summary sales report by customer segment.
- Schedules this workflow with advanced triggers and practices recovery from job failures.

---

## Step-by-Step Guide

### Step 1: Upload Files to DBFS

> **Note:** In your environment, copy `sales.csv` and `customer_demographics.csv` to `/dbfs/FileStore/lab_data/`.

---

### Step 2: Create Notebooks

#### Notebook 1: Ingest Sales Data (`01_ingest_sales_data`)

```python

sales_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/lab_data/sales.csv")

print("Sales Data (First 5 rows):")
display(sales_df.limit(5))

# Output as JSON to a file for the next task
json_output_path = "dbfs:/FileStore/lab_data/sales_output.json"
sales_df.write.mode("overwrite").json(json_output_path)
dbutils.jobs.taskValues.set(key="ingested_data_json", value=json_output_path)
# Output the file path for the next task
dbutils.notebook.exit("Ingestion complete!")
```

#### Notebook 2: Ingest Customer Demographics (`02_ingest_customer_data`)

```python
demog_path = 'dbfs:/FileStore/lab_data/customer_demographics.csv'
demog_df = spark.read.option("header", True)\
    .option("escape", '"')\
    .option("quote", '"')\
    .option("multiLine", True)\
    .option("inferSchema", True).csv(demog_path)

print("Customer Demographics (First 5 rows):")
display(demog_df.limit(5))

# Output as JSON for next task using dbutils.jobs.taskValues.set()
json_output_path = "dbfs:/FileStore/lab_data/customer_output.json"
demog_df.write.mode("overwrite").json(json_output_path)

dbutils.jobs.taskValues.set(key="ingested_customer_data_json", value=json_output_path)

# Output the file path for the next task
dbutils.notebook.exit("Customer Ingestion Complete!")
```

#### Notebook 3: Transform and Merge Data (`03_transform_and_merge`)

```python
%python
dbutils.widgets.text("sales_json", "", "Sales JSON")
dbutils.widgets.text("demog_json", "", "Demographics JSON")

# Get inputs
sales_json = dbutils.widgets.get("sales_json")
demog_json = dbutils.widgets.get("demog_json")

sales_df = spark.read.json(sales_json)
demog_df = spark.read.json(demog_json)

# Clean Data (example: drop nulls)
sales_df_clean = sales_df.na.drop()
demog_df_clean = demog_df.na.drop()

# Merge on Customer or common column (assume 'CustomerID')
merged = sales_df_clean.join(demog_df_clean, "CustomerID", "inner")
print("Merged Data (First 5 rows):")
display(merged.limit(5))

# Output as JSON for next task using dbutils.jobs.taskValues.set()
json_output_path = "dbfs:/FileStore/lab_data/merge_output.json"
merged.write.mode("overwrite").json(json_output_path)

dbutils.jobs.taskValues.set(key="merged_data_json", value=json_output_path)

# Output for reporting
dbutils.notebook.exit("Merge Complete!")
```

#### Notebook 4: Generate Report (`04_generate_report`)

```python
# Receive merged data path from previous notebook
dbutils.widgets.text("merged_json", "", "Merged Data Path")
merged_path = dbutils.widgets.get("merged_json")

# Read merged data as a Spark DataFrame
final_merged = spark.read.json(merged_path)

# Check for column names and map them to correct capitalization
columns = [c.lower() for c in final_merged.columns]
if "region" not in columns:
    raise Exception("Column 'Region' not found in merged data.")
# Find the real column name (to support any capitalization)
region_col = [c for c in final_merged.columns if c.lower() == "region"][0]
unit_price_col = [c for c in final_merged.columns if c.lower() == "unitprice"][0]

from pyspark.sql.functions import col, sum as spark_sum, countDistinct

# Group and aggregate
report = (
    final_merged
    .groupBy(region_col)
    .agg(
        spark_sum(unit_price_col).alias("TotalSales"),
        countDistinct("SalesOrderNumber").alias("OrderCount")
    )
    .orderBy(col("TotalSales").desc())
)
print("")
display(report)

# Save report to CSV (single file)
report_path = "dbfs:/FileStore/lab_data/region_sales_report.csv"
report.coalesce(1).write.mode("overwrite").option("header", True).csv(report_path)

# Output row count for next task
row_count = report.count()
display(row_count)
#dbutils.notebook.exit("Report Generated!")

```

---

### Step 3: Build the Multi-Task Workflow in Databricks

- **Navigate to Jobs & Pipeline:** Select Job `CustomerSalesAnalyticsWorkflow`
- **Job Name:** `CustomerSalesAnalyticsWorkflow`
- **Tasks:**

1. Set the job type to **Notebook**.
2. Set the Path to your notebook (e.g., `/Users/<your_username>/MyFirstNotebook`).
3. Use **Jobs Compute** as the cluster type created in earlier labs.
4. Ingest Sales Data (Notebook 1)
5. Ingest Customer Data (Notebook 2)
6. Transform and Merge (Notebook 3)
   - Depends on Task 1 & 2
   - Pass outputs as inputs
     - key: sales_json, Value: {{tasks.Ingest_Sales_Data.values.ingested_data_json}}
     - key: demog_json, Value: {{tasks.Ingest_Customer_Data.values.ingested_customer_data_json}}
7. Generate Report (Notebook 4)
   - Depends on Task 3
   - Pass merged_json as input
     - key: merged_json, Value: {{tasks.Transform_and_Merge.values.merged_data_json}}

- **Cluster Type:** Use Jobs Compute for all tasks.
- **Visualize DAG:** Confirm fork/join pattern.

---

### Step 4: Advanced Scheduling and Notifications

- **Schedules:**
  - Daily at 7 AM: `0 7 * * * *`
  - Every Monday at 8 PM: `0 20 * * 1`
- **Notifications:** Configure email for success and failure events.

---

### Step 5: Test & Validate

- Run the workflow manually.
- Check outputs at each stage.
- Validate the report file.
- Test schedule triggers if possible.

---

### Step 6: Error Simulation & Recovery

- In Notebook 3, add:
  ```python
  raise Exception("Simulated pipeline failure!")
  ```
- Rerun workflow; confirm failure at transform stage.
- Review failure notification.
- Remove error and use **Repair Run** to resume from failed task.

---

### Step 7: Documentation and Tags

- Add markdown headers to each notebook:
  - Purpose, input, output, author, date.
- Tag your job in the UI for tracking:  
  `purpose: lab`, `owner: <your name>`, `env: dev`

---

## Lab Challenge

### Lab Challenge: Multi-Source ETL Pipeline

#### Scenario

Design and implement a Databricks workflow that ingests, merges, transforms, and reports on data from two sources: sales and marketing.

#### Requirements

1. **Tasks & Notebooks:**

   - **Ingest Sales Data:** Notebook reads sales data (`source=sales`).
   - **Ingest Product Data:** Notebook reads product data (`source=product`).
   - **Merge & Transform Data:** Depends on both ingestion tasks. Merges DataFrames, applies filters, and adds a calculated column.
   - **Generate Combined Report:** Depends on merge task. Aggregates data and saves a combined report.

2. **Workflow Configuration:**

   - Use Jobs Compute for all tasks.
   - Pass outputs between tasks using parameters/JSON.
   - Schedules:
     - Every Friday at 9 PM: `0 21 * * 5`
   - Configure notifications for job failure.

3. **Error Handling:**

   - Simulate a failure in the merge/transform task (e.g., `raise Exception("Simulated failure")`).
   - Use Repair Run to recover after fixing the error.

4. **Documentation & Tags:**

   - Add markdown headers to each notebook (purpose, input, output, author, date).
   - Tag the job in the UI (e.g., `purpose: challenge`, `owner: <your name>`, `env: dev`).

5. **Bonus:**
   - Add a final task to archive the report (e.g., move to `/dbfs/FileStore/archive/`) that runs only if all previous tasks succeed (conditional execution).

#### Deliverables

- Workflow configuration (describe or screenshot).
- All notebook code with documentation headers.
- Evidence of error simulation and recovery.
- Tags and schedule settings.
- (Bonus) Archive task implementation.

---
