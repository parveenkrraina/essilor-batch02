# Session 2 Lab: Data Cleaning & Basic Transformations

## Step 1: Load the raw CSV data

```python
csv_path = "dbfs:/FileStore/data/sales.csv"

sales_df = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true")\
    .option("escape", '"')\
    .option("quote", '"')\
    .option("multiLine", True)\

print("Raw data sample:")
sales_df.show(5, truncate=False)
```

---

## Step 2: Remove duplicates and handle missing values

```python
from pyspark.sql.functions import col

clean_df = sales_df.dropDuplicates() \
    .dropna(subset=["Quantity", "CustomerName"]) \
    .withColumn("Quantity", col("Quantity").cast("int")) \
    .withColumn("UnitPrice", col("UnitPrice").cast("float"))

print("Cleaned data preview:")
clean_df.show(5, truncate=False)
```

---

## Step 3: Clean and parse the JSON column `ProductMetadata`

```python
from pyspark.sql.functions import from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# Clean JSON string in ProductMetadata
clean_df = clean_df.withColumn(
    "ProductMetadata_clean",
    regexp_replace(col("ProductMetadata"), '^"+|"+$', '')
).withColumn(
    "ProductMetadata_clean",
    regexp_replace(col("ProductMetadata_clean"), '""', '"')
)

# Define schema for JSON parsing
json_schema = StructType([
    StructField("color", StringType(), True),
    StructField("warranty", StringType(), True)
])

# Parse JSON column
clean_df = clean_df.withColumn("ProductDetails", from_json(col("ProductMetadata_clean"), json_schema))

# Extract fields
clean_df = clean_df.withColumn("color", col("ProductDetails.color")) \
    .withColumn("warranty", col("ProductDetails.warranty"))

print("Data with parsed JSON fields:")
clean_df.select("SalesOrderNumber", "color", "warranty").show(5)
```

---

## Step 4: Save cleaned data as Delta table

```python
clean_df.write.format("delta").mode("overwrite").saveAsTable("sales_cleaned")
print("Delta table 'sales_cleaned' created.")
```

---

## Step 5: Perform aggregations (e.g., total sales per customer)

```python
from pyspark.sql.functions import sum as _sum

agg_df = clean_df.groupBy("CustomerName").agg(
    _sum(col("Quantity") * col("UnitPrice")).alias("TotalSales")
)

print("Aggregated sales per customer:")
agg_df.show(10)
```
#