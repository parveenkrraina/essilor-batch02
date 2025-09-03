# Databricks Workflows: Comprehensive Hands-On Guide

Welcome to the hands-on lab for **Module 2: Deploy Workloads with Databricks Workflows!**

In this module, you will learn to build, schedule, and manage robust data pipelines using Databricks Workflows. Topics include creating jobs with multiple tasks, conditional logic, parameterization, task orchestration in loops, repairing failed runs, and best practices for modular workflow design.

---

## Learning Objectives

- Understand Databricks Workflows components and benefits
- Configure job clusters for efficient execution
- Create/manage jobs with various task types (Notebooks)
- Implement conditional execution with "If/Else Condition" and task value passing
- Use "For Each" tasks to iterate/process data dynamically
- Parameterize jobs and tasks
- Monitor job runs, troubleshoot failures, and repair runs
- Design modular workflows for maintainability and reusability

---

## Prerequisites

- Access to an Azure Databricks workspace
- Downloaded CSV files: `customer_demographics.csv`, `products.csv`, `sales.csv`
- Basic understanding of PySpark and SQL

---

## Part 1: Data Setup

### Step 1.1: Upload CSV Files to DBFS

1. In Databricks, go to **Catalog** > **DBFS** tab.
2. Create folder `/FileStore/lab_data/` if it doesn't exist.
3. Upload:
    - `customer_demographics.csv`
    - `products.csv`
    - `sales.csv`

### Step 1.2: Create Bronze Delta Tables

Create a notebook named `00_setup_ingest_raw_data`, attach to a cluster, and run:

```python
# Notebook: 00_setup_ingest_raw_data
base_path = "dbfs:/FileStore/lab_data/"
customer_csv_path = f"{base_path}customer_demographics.csv"
products_csv_path = f"{base_path}products.csv"
sales_csv_path = f"{base_path}sales.csv"

db_name = "module2_db"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")

customer_table_name = "bronze_customers"
products_table_name = "bronze_products"
sales_table_name = "bronze_sales"

# Ingest Customer Demographics
df_customers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(customer_csv_path)
df_customers.write.format("delta").mode("overwrite").saveAsTable(customer_table_name)

# Ingest Products
df_products = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(products_csv_path)
df_products.write.format("delta").mode("overwrite").saveAsTable(products_table_name)

# Ingest Sales
df_sales = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(sales_csv_path)
df_sales.write.format("delta").mode("overwrite").saveAsTable(sales_table_name)
```

This creates three Delta tables in `module2_db`.

---

## Part 2: Creating the Workflow Notebooks

Create these Python notebooks in your workspace:

### Notebook 1: N1_Validate_Sales_Data

**Purpose:** Validates `bronze_sales`. Sets task value `validation_status` ("VALID"/"INVALID").

<details>
<summary>Show code</summary>

```python
# N1_Validate_Sales_Data
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, DateType, DoubleType

dbutils.widgets.text("sales_table_name", "bronze_sales")
dbutils.widgets.text("database_name", "module2_db")
dbutils.widgets.dropdown("force_valid_status_for_testing", "False", ["True", "False"])

sales_table = dbutils.widgets.get("sales_table_name")
db_name = dbutils.widgets.get("database_name")
force_valid_status = dbutils.widgets.get("force_valid_status_for_testing") == "True"
full_sales_table_name = f"{db_name}.{sales_table}"

try:
     df_sales_raw = spark.table(full_sales_table_name)
except Exception as e:
     dbutils.taskValues.set(key="validation_status", value="ERROR_READING_TABLE")
     dbutils.taskValues.set(key="validation_error_message", value=str(e))
     dbutils.notebook.exit(f"Failed to read table: {full_sales_table_name}")

df_sales = df_sales_raw
if 'OrderDate' in df_sales.columns and dict(df_sales.dtypes)['OrderDate'] == 'string':
     df_sales = df_sales.withColumn("OrderDate_casted", to_date(col("OrderDate")))
else:
     df_sales = df_sales.withColumn("OrderDate_casted", col("OrderDate").cast(DateType()))
df_sales = df_sales.withColumn("Quantity_casted", col("Quantity").cast(IntegerType()))
df_sales = df_sales.withColumn("UnitPrice_casted", col("UnitPrice").cast(DoubleType()))

required_cols = ["SalesOrderNumber", "OrderDate_casted", "CustomerName", "Item", "Quantity_casted", "UnitPrice_casted"]
missing_cols_source = [c.replace("_casted","") for c in required_cols if c.replace("_casted","") not in df_sales_raw.columns]

if missing_cols_source:
     error_message = f"Missing source columns: {', '.join(missing_cols_source)}"
     dbutils.taskValues.set(key="validation_status", value="INVALID_SCHEMA")
     dbutils.taskValues.set(key="validation_error_message", value=error_message)
     dbutils.notebook.exit(error_message)

null_sales_order_number_count = df_sales.filter(col("SalesOrderNumber").isNull()).count()
null_order_date_count = df_sales.filter(col("OrderDate_casted").isNull()).count()
null_customer_name_count = df_sales.filter(col("CustomerName").isNull()).count()
null_item_count = df_sales.filter(col("Item").isNull()).count()
invalid_quantity_count = df_sales.filter(col("Quantity_casted").isNull() | (col("Quantity_casted") <= 0)).count()
null_unit_price_count = df_sales.filter(col("UnitPrice_casted").isNull() | (col("UnitPrice_casted") < 0)).count()

validation_status_actual = "VALID"
error_message = ""
if null_sales_order_number_count > 0: validation_status_actual = "INVALID"; error_message += f"Null SalesOrderNumber. "
if null_order_date_count > 0: validation_status_actual = "INVALID"; error_message += f"Null/invalid OrderDate. "
if null_customer_name_count > 0: validation_status_actual = "INVALID"; error_message += f"Null CustomerName. "
if null_item_count > 0: validation_status_actual = "INVALID"; error_message += f"Null Item. "
if invalid_quantity_count > 0: validation_status_actual = "INVALID"; error_message += f"Null/non-positive Quantity. "
if null_unit_price_count > 0: validation_status_actual = "INVALID"; error_message += f"Null/negative UnitPrice. "

final_validation_status = "VALID" if force_valid_status else validation_status_actual
final_error_message = "Forced VALID for testing" if force_valid_status else error_message

dbutils.taskValues.set(key="validation_status", value=final_validation_status)
dbutils.taskValues.set(key="source_table_record_count", value=df_sales_raw.count())
if final_validation_status == "INVALID":
     dbutils.taskValues.set(key="validation_error_message", value=final_error_message)
dbutils.notebook.exit(final_validation_status)
```
</details>

---

### Notebook 2: N2_Process_Sales_Data

**Purpose:** Joins sales, customer, and product data if validation passes. Calculates revenue and saves to a "silver" table.

<details>
<summary>Show code</summary>

```python
# N1_Validate_Sales_Data
from pyspark.sql.functions import col, count, to_date
from pyspark.sql.types import IntegerType, DateType

dbutils.widgets.text("sales_table_name", "bronze_sales", "Source Sales Table")
dbutils.widgets.text("database_name", "module2_db", "Database Name")
# ADD A WIDGET TO CONTROL THE OVERRIDE BEHAVIOR
dbutils.widgets.dropdown("force_valid_status_for_testing", "False", ["True", "False"], "Force Valid Status for Testing?")


sales_table = dbutils.widgets.get("sales_table_name")
db_name = dbutils.widgets.get("database_name")
force_valid_status = dbutils.widgets.get("force_valid_status_for_testing") == "True" # Get boolean from widget

full_sales_table_name = f"{db_name}.{sales_table}"

print(f"Validating table: {full_sales_table_name}")
if force_valid_status:
    print("WARNING: Validation status will be forced to 'VALID' for testing purposes based on widget setting.")

try:
    df_sales = spark.table(full_sales_table_name)
except Exception as e:
    print(f"Error reading table {full_sales_table_name}: {e}")
    dbutils.taskValues.set(key="validation_status", value="ERROR_READING_TABLE")
    dbutils.taskValues.set(key="validation_error_message", value=str(e))
    dbutils.notebook.exit(f"Failed to read table: {full_sales_table_name}")

# YOUR SALES COLUMNS: SalesOrderNumber, OrderDate, CustomerName, EmailAddress, Item, Quantity, UnitPrice, TaxAmount

# Attempt to cast relevant columns to expected types for validation
if dict(df_sales.dtypes)['OrderDate'] == 'string':
    df_sales = df_sales.withColumn("OrderDate", to_date(col("OrderDate"))) 

if dict(df_sales.dtypes)['Quantity'] == 'string': # Ensure Quantity is numeric before checking <= 0
     df_sales = df_sales.withColumn("Quantity", col("Quantity").cast(IntegerType()))


required_cols = ["SalesOrderNumber", "OrderDate", "CustomerName", "Item", "Quantity", "UnitPrice"]
missing_cols = [c for c in required_cols if c not in df_sales.columns]

if missing_cols:
    error_message = f"Missing required columns for validation: {', '.join(missing_cols)}"
    print(f"VALIDATION_ERROR: {error_message}")
    dbutils.taskValues.set(key="validation_status", value="INVALID_SCHEMA")
    dbutils.taskValues.set(key="validation_error_message", value=error_message)
    dbutils.notebook.exit(error_message)

null_sales_order_number_count = df_sales.filter(col("SalesOrderNumber").isNull()).count()
null_order_date_count = df_sales.filter(col("OrderDate").isNull()).count()
null_customer_name_count = df_sales.filter(col("CustomerName").isNull()).count()
null_item_count = df_sales.filter(col("Item").isNull()).count()
# This is the problematic line causing the error message.
# Let's make sure Quantity column exists and is numeric before this filter
if "Quantity" in df_sales.columns and isinstance(df_sales.schema["Quantity"].dataType, IntegerType):
    invalid_quantity_count = df_sales.filter(col("Quantity").isNull() | (col("Quantity") <= 0)).count()
else:
    invalid_quantity_count = 0 # Or handle as schema error if Quantity is mandatory and not integer
    if "Quantity" not in df_sales.columns:
        print("WARNING: Quantity column not found. Skipping quantity validation.")
    else:
        print(f"WARNING: Quantity column is not IntegerType (it's {df_sales.schema['Quantity'].dataType}). Skipping non-positive quantity validation.")


null_unit_price_count = df_sales.filter(col("UnitPrice").isNull() | (col("UnitPrice") < 0)).count()


validation_status_actual = "VALID" # Assume valid initially
error_message = ""

if null_sales_order_number_count > 0:
    validation_status_actual = "INVALID"
    error_message += f"Found {null_sales_order_number_count} records with null SalesOrderNumber. "
if null_order_date_count > 0:
    validation_status_actual = "INVALID"
    error_message += f"Found {null_order_date_count} records with null or invalid OrderDate. "
if null_customer_name_count > 0:
    validation_status_actual = "INVALID"
    error_message += f"Found {null_customer_name_count} records with null CustomerName. "
if null_item_count > 0:
    validation_status_actual = "INVALID"
    error_message += f"Found {null_item_count} records with null Item. "
if invalid_quantity_count > 0: # This condition will still be evaluated
    validation_status_actual = "INVALID"
    # This is the error you were seeing
    error_message += f"Found {invalid_quantity_count} records with null or non-positive Quantity. "
if null_unit_price_count > 0:
    validation_status_actual = "INVALID"
    error_message += f"Found {null_unit_price_count} records with null or negative UnitPrice. "

# --- MODIFICATION FOR TESTING ---
# If force_valid_status widget is set to True, override the actual validation status.
if force_valid_status:
    final_validation_status = "VALID"
    print(f"Actual validation status was: {validation_status_actual}. Error messages (if any): {error_message}")
    print("OVERRIDING to 'VALID' for testing flow.")
    error_message = "Forced VALID for testing (actual was " + validation_status_actual + ")" if validation_status_actual == "INVALID" else ""

else:
    final_validation_status = validation_status_actual

# Set the task values based on the final_validation_status
dbutils.jobs.taskValues.set(key="validation_status", value=final_validation_status)
dbutils.jobs.taskValues.set(key="source_table_record_count", value=df_sales.count())

if final_validation_status == "INVALID":
    # Only set error message if it's genuinely invalid, or the forced message
    dbutils.jobs.taskValues.set(key="validation_error_message", value=error_message)
    print(f"Validation result: {final_validation_status}. Reason: {error_message}")
else:
    print(f"Validation result: {final_validation_status}.")
    if error_message: # This would be the "Forced VALID..." message
         dbutils.jobs.taskValues.set(key="validation_error_message", value=error_message)


#dbutils.notebook.exit(final_validation_status)
```
</details>

---

### Notebook 3: N2b_Handle_Invalid_Data

**Purpose:** Executed if sales data validation fails. Logs error details.

<details>
<summary>Show code</summary>

```python
# N2b_Handle_Invalid_Data

# This notebook is called when sales data validation fails.
dbutils.widgets.text("validator_task_key", "Task_Validate_Sales", "Task key of the validation notebook")
validator_task_key = dbutils.widgets.get("validator_task_key")

print("Handling invalid data scenario...")

try:
    error_message = dbutils.taskValues.get(taskKey=validator_task_key, key="validation_error_message", default="No error message provided.", debugValue="Debug error message.")
    source_count = dbutils.taskValues.get(taskKey=validator_task_key, key="source_table_record_count", default=0, debugValue=0)
    
    print(f"Data validation failed for task '{validator_task_key}'.")
    print(f"Source record count: {source_count}")
    print(f"Error details: {error_message}")
    
    # Simulate sending a notification or logging to a specific system
    print("NOTIFICATION: Sales data validation failed. Please check the logs and data quality.")
    # In a real scenario, you might integrate with PagerDuty, Slack, or email here.

    dbutils.taskValues.set(key="invalid_data_handling_status", value="COMPLETED_NOTIFICATION")
    dbutils.notebook.exit("Invalid data handling complete. Notification simulated.")

except Exception as e:
    print(f"Error in N2b_Handle_Invalid_Data: {e}")
    #dbutils.notebook.exit(f"Failed to handle invalid data: {e}")
```
</details>

---

### Notebook 4: N3_Archive_Raw_Sales

**Purpose:** Archives the raw sales data. Runs regardless of validation outcome.

<details>
<summary>Show code</summary>

```python
# N3_Archive_Raw_Sales

dbutils.widgets.text("sales_table_name", "bronze_sales", "Source Sales Table")
dbutils.widgets.text("archive_path_base", "dbfs:/FileStore/lab_data/archived_sales/", "Base Archive Path") # Updated path
dbutils.widgets.text("database_name", "module2_db", "Database Name")
dbutils.widgets.text("validator_task_key", "Task_Validate_Sales", "Task key of the validation notebook")


sales_table = dbutils.widgets.get("sales_table_name")
archive_path_base = dbutils.widgets.get("archive_path_base")
db_name = dbutils.widgets.get("database_name")
validator_task_key = dbutils.widgets.get("validator_task_key")
full_sales_table_name = f"{db_name}.{sales_table}"

from datetime import datetime
timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
archive_path_final = f"{archive_path_base.rstrip('/')}/{timestamp_str}/"

print(f"Archiving table {full_sales_table_name} to {archive_path_final}")

try:
    # Getting values for context, not for conditional logic here as this task runs regardless.
    validation_status = dbutils.taskValues.get(taskKey=validator_task_key, key="validation_status", default="N/A_STATUS", debugValue="DEBUG_ARCHIVE")
    source_count = dbutils.taskValues.get(taskKey=validator_task_key, key="source_table_record_count", default=0, debugValue=0)
    print(f"Upstream validation status for context: {validation_status}")
    print(f"Upstream source record count for context: {source_count}")
except Exception as e:
    print(f"Could not retrieve some task values from '{validator_task_key}': {e}.")


try:
    df_sales = spark.table(full_sales_table_name)
    df_sales.write.format("parquet").mode("overwrite").save(archive_path_final)
    archived_count = df_sales.count()
    print(f"Successfully archived {archived_count} records from {full_sales_table_name} to {archive_path_final}")
    dbutils.taskValues.set(key="archived_path", value=archive_path_final)
    dbutils.taskValues.set(key="archived_count", value=archived_count)
    dbutils.notebook.exit(f"Archiving task finished. Path: {archive_path_final}")

except Exception as e:
    error_msg = f"Failed to archive {full_sales_table_name}: {e}"
    print(error_msg)
    dbutils.notebook.exit(error_msg) # Exiting with error to ensure job reflects failure if archive is critical
```
</details>

---

### Notebook 5: N4_Process_Sales_By_Category

**Purpose:** Processes enriched sales data for a specific product category (used in a "For Each" loop).

<details>
<summary>Show code</summary>

```python
# N4_Process_Sales_By_Category
from pyspark.sql.functions import col

dbutils.widgets.text("category_filter", "", "Category to filter and process (populated by {{item}})")
dbutils.widgets.text("enriched_sales_table", "silver_sales_enriched", "Enriched Sales Table") # This is df_output from N2
dbutils.widgets.text("output_path_base", "dbfs:/FileStore/lab_data/category_sales/", "Base output path for category sales")
dbutils.widgets.text("database_name", "module2_db", "Database Name")

current_category_widget = dbutils.widgets.get("category_filter")
current_category = current_category_widget 

enriched_sales_table_name = dbutils.widgets.get("enriched_sales_table")
output_path_base = dbutils.widgets.get("output_path_base")
db_name = dbutils.widgets.get("database_name")

full_enriched_sales_table_path = f"{db_name}.{enriched_sales_table_name}"
safe_category_name = "".join(c if c.isalnum() else "_" for c in current_category)
category_output_path = f"{output_path_base.rstrip('/')}/{safe_category_name}/"

print(f"Processing sales for category: {current_category} (Safe name: {safe_category_name})")
print(f"Input table: {full_enriched_sales_table_path}")
print(f"Output path: {category_output_path}")

if not current_category:
    dbutils.notebook.exit("Category not provided. Exiting.")

try:
    df_enriched_sales = spark.table(full_enriched_sales_table_path)
except Exception as e:
    print(f"Error reading table {full_enriched_sales_table_path}: {e}")
    dbutils.notebook.exit(f"Failed to read table: {full_enriched_sales_table_path}")

# The silver_sales_enriched table (df_output in N2) contains 'Category' from products table.
if "Category" not in df_enriched_sales.columns:
    dbutils.notebook.exit(f"Enriched sales table missing 'Category' column for filtering.")

df_category_sales = df_enriched_sales.filter(col("Category") == current_category)

if df_category_sales.isEmpty():
    print(f"No sales found for category: {current_category}")
    dbutils.notebook.exit(f"No sales for category {current_category}")
else:
    df_category_sales.write.format("delta").mode("overwrite").save(category_output_path) # Save as Delta files, not table
    count = df_category_sales.count()
    print(f"Successfully processed and saved {count} sales records for category '{current_category}' to {category_output_path}")
    dbutils.taskValues.set(key=f"processed_count_{safe_category_name}", value=count)
    dbutils.notebook.exit(f"Processed category {current_category}")
```
</details>

---
## Part 3: Building the Databricks Workflow (Job)

Follow these steps to create the workflow in Databricks:

1. **Navigate to Workflows:** In the left sidebar, click **Workflows**.

2. **Create Job:** Click **Create Job**.

3. **Job Name:** Enter `Sales Pipeline Lab - [Your Name]`.

4. **Configure Job Cluster:**
    - Select **New job cluster**.
    - Choose a recent LTS Databricks Runtime (e.g., 13.3 LTS, 14.3 LTS).
    - Node type: e.g., `Standard_DS3_v2` (Azure) or `m5.large` (AWS).
    - Workers: 1.
    - Set auto-termination to 20 minutes.
    - Click **Confirm**.

5. **Add Tasks:**

    - **Task_Validate_Sales**
      - Type: Notebook (`N1_Validate_Sales_Data`)
      - Parameters:
         - `database_name`: `module2_db`
         - `sales_table_name`: `bronze_sales`
         - `force_valid_status_for_testing`: `True` (for initial test)
      - Cluster: [Job cluster]

    - **Task_IfElse_Validation**
      - Type: If/Else condition
      - Depends on: `Task_Validate_Sales`
      - Condition: `{{tasks.Task_Validate_Sales.values.validation_status}} == "VALID"`
      - **True branch:** Add `Task_Process_Sales`
         - Notebook: `N2_Process_Sales_Data`
         - Parameters:
            - `database_name`: `module2_db`
            - `sales_table_name`: `bronze_sales`
            - `customers_table_name`: `bronze_customers`
            - `products_table_name`: `bronze_products`
            - `output_table_name`: `silver_sales_enriched`
      - **False branch:** Add `Task_Handle_Invalid_Data`
         - Notebook: `N2b_Handle_Invalid_Data`
         - Parameters:
            - `validator_task_key`: `Task_Validate_Sales`

    - **Task_Archive_Sales**
      - Type: Notebook (`N3_Archive_Raw_Sales`)
      - Depends on: `Task_Validate_Sales`
      - "Run if": **All upstream done**
      - Parameters:
         - `database_name`: `module2_db`
         - `sales_table_name`: `bronze_sales`
         - `archive_path_base`: `dbfs:/FileStore/lab_data/archived_sales/`
         - `validator_task_key`: `Task_Validate_Sales`

    - **Task_Loop_By_Category**
      - Type: For Each (after `Task_Process_Sales` in the True branch)
      - Input list: e.g., `["Electronics", "Apparel", "Home Goods", "Books", "Sports"]` (adjust based on your data)
      - Child task: `SubTask_Process_Category_Data`
         - Notebook: `N4_Process_Sales_By_Category`
         - Parameters:
            - `category_filter`: `{{item}}`
            - `enriched_sales_table`: `silver_sales_enriched`
            - `output_path_base`: `dbfs:/FileStore/lab_data/category_sales/`
            - `database_name`: `module2_db`
      - (Optional) Set concurrency to 2.

6. **Job Parameters (optional):**
    - Add a parameter: `run_date` with default `{{current_date}}`.

7. **Notifications:**
    - Add email notifications for job success and failure.

8. **Scheduling (optional):**
    - Set a daily schedule if desired.

Click **Create** or **Save** to finish configuring the job.

---

## Part 4: Running and Monitoring the Workflow

### First Run (Testing Valid Path)

1. **Set Up for Test:**
    - Ensure `Task_Validate_Sales` has `force_valid_status_for_testing` set to `True`.

2. **Run the Job:**
    - Go to your job: `Sales Pipeline Lab - [Your Name]`.
    - Click **Run now** in the top right.

3. **Monitor the Run:**
    - In the "Runs" tab, a new run will appear. Click on it.
    - Observe the workflow graph:
      - `Task_Validate_Sales` runs first.
      - `Task_IfElse_Validation` evaluates the result.
      - Since validation is forced to `"VALID"`, the **True** branch (`Task_Process_Sales`) executes, followed by `Task_Loop_By_Category`.
      - `Task_Archive_Sales` runs regardless of validation outcome.
    - Click each task to view status, logs, and task values.

4. **Check Outputs:**
    - In **Data > Data Explorer**, check the `module2_db` database for the `silver_sales_enriched` table and query it.
    - In **DBFS**, verify:
      - `dbfs:/FileStore/lab_data/archived_sales/` contains a timestamped folder with Parquet files.
      - `dbfs:/FileStore/lab_data/category_sales/` contains folders for each category with Delta files.

---

## Part 5: Testing Error Handling and Actual Validation

### Second Run (Actual Validation)

1. **Edit the Job:**
    - Go to your job: `Sales Pipeline Lab - [Your Name]`.
    - Edit `Task_Validate_Sales` and set `force_valid_status_for_testing` to `False`. Save the task and the job.

2. **Run the Job:**
    - Click **Run now**.

3. **Observe Behavior:**
    - `Task_Validate_Sales` performs real validation.
    - If your `sales.csv` has issues (e.g., non-positive Quantity), `validation_status` will be `"INVALID"`.
    - The workflow takes the **False** branch of `Task_IfElse_Validation`, running `Task_Handle_Invalid_Data`.
    - `Task_Process_Sales` and `Task_Loop_By_Category` are skipped.
    - `Task_Archive_Sales` still runs.
    - Check the logs of `Task_Handle_Invalid_Data` for error messages.

### Simulating a Repair Scenario (Optional)

1. **Introduce a Temporary Error:**
    - In `N1_Validate_Sales_Data`, add a line like `x = 1/0` to cause a failure.

2. **Run the Job:**
    - The job will fail at `Task_Validate_Sales`.

3. **Repair the Run:**
    - Fix the error in the notebook and save it.
    - In the failed run UI, click **Repair run**.
    - Choose to rerun failed tasks. The job resumes from the failed task and continues.

---

## Part 6: (Optional) Modular Orchestration Demo

This section demonstrates how to modularize workflows by calling one Databricks job from another, enabling reusable utility patterns.

### Step 1: Create Utility Notebook

**Notebook:** `N5_Utility_Archive_Table`

<details>
<summary>Show code</summary>

```python
# N5_Utility_Archive_Table
from datetime import datetime
dbutils.widgets.text("db_name", "", "Database Name")
dbutils.widgets.text("table_name", "", "Table Name to Archive")
dbutils.widgets.text("archive_base_path", "dbfs:/FileStore/lab_data/utility_archives/", "Base Archive Path")

db_name_param = dbutils.widgets.get("db_name")
table_name_param = dbutils.widgets.get("table_name")
archive_base_path_param = dbutils.widgets.get("archive_base_path")

if not db_name_param or not table_name_param:
    dbutils.notebook.exit("ERROR: Database name and table name are required.")

full_table_name = f"{db_name_param}.{table_name_param}"
timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
output_path = f"{archive_base_path_param.rstrip('/')}/{db_name_param}/{table_name_param}/{timestamp_str}/"

print(f"Archiving table: {full_table_name} to {output_path}")
try:
    df = spark.table(full_table_name)
    df.write.format("parquet").mode("overwrite").save(output_path)
    print(f"Successfully archived {df.count()} records from {full_table_name} to {output_path}")
    dbutils.taskValues.set("archived_utility_path", output_path)
    dbutils.notebook.exit(f"Archive successful: {output_path}")
except Exception as e:
    print(f"Error archiving table {full_table_name}: {e}")
    dbutils.notebook.exit(f"Archive failed: {e}")
```
</details>

---

### Step 2: Create Utility Job

**Job Name:** `Utility - Generic Table Archiver`

- **Task 1:** Runs `N5_Utility_Archive_Table`
    - Parameters:
        - `db_name`: `{{job.parameters.target_db}}`
        - `table_name`: `{{job.parameters.target_table}}`
        - `archive_base_path`: `{{job.parameters.target_archive_root}}`
- **Job Parameters:**
    - `target_db` (e.g., default: `module2_db`)
    - `target_table` (e.g., default: `silver_sales_enriched`)
    - `target_archive_root` (e.g., default: `dbfs:/FileStore/lab_data/utility_archives/`)

---

### Step 3: Create Main Job That Calls the Utility Job

**Job Name:** `Main - Process and Call Archiver`

- **Task 1:** (Processing task, e.g., `Task_Process_Sales` from earlier)
- **Task 2:** `Task_Call_Utility_Archiver`
    - Type: **Run Job**
    - Run Job: Select `Utility - Generic Table Archiver`
    - Parameters:
        - `target_db`: `module2_db`
        - `target_table`: `silver_sales_enriched`
        - `target_archive_root`: `dbfs:/FileStore/lab_data/main_job_called_archives/`
    - Depends on: Task 1

---

### Step 4: Run and Observe

- Run `Main - Process and Call Archiver`.
- After the processing task completes, it triggers the utility job to archive the specified table.
- Check the archive location for output files.

---

**End of Lab**
