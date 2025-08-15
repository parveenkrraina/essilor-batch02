# Session 3 Lab: SQL UDFs, Schema Evolution, Time Travel, Optimization

## Step 6: Create a SQL UDF to Uppercase Customer Names

```sql
CREATE OR REPLACE FUNCTION to_upper_case(str STRING) RETURNS STRING
RETURN UPPER(str);
```

## Step 7: Use the UDF to Create a Standardized Customer Name Column

```python
from pyspark.sql.functions import expr

df_with_std_name = spark.table("sales_cleaned").withColumn(
    "CustomerNameStd",
    expr("to_upper_case(CustomerName)")
)

df_with_std_name.write.format("delta").mode("overwrite").saveAsTable("sales_final")
print("Delta table 'sales_final' with standardized customer names created.")
```

## Step 8: Add a New Column `Discount` Using Schema Evolution

```python
from pyspark.sql.functions import lit

df_with_discount = spark.table("sales_final").withColumn("Discount", lit(0.1))

df_with_discount.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("sales_final")

print("Added 'Discount' column to 'sales_final' table.")
```

## Step 9: Query Previous Table Version Using Time Travel

```sql
SELECT * FROM sales_final VERSION AS OF 0 LIMIT 10;
```

## Step 10: Optimize the Delta Table and Apply Z-ordering on Customer Name

```sql
OPTIMIZE sales_final ZORDER BY (CustomerNameStd);
```
## Challenge 1: Custom SQL UDF for Warranty Extraction

- Create a SQL UDF that takes a `ProductMetadata` JSON string as input.
- The UDF should extract and return the warranty period, or return "No Warranty" if missing.
- Use this UDF in a query to augment the sales table with warranty info.

---

## Challenge 2: Schema Evolution & Time Travel

- Add a new nullable column `ReturnReason` to the sales Delta table.
- Update some rows with return reasons.
- Use time travel to query the table before and after the schema change, showing differences.

---

## Challenge 3: Optimize & Z-order

- Run a slow query filtering sales by `CustomerName`.
- Optimize the table using Z-order on `CustomerName`.
- Compare query runtimes before and after optimization, documenting improvements.
