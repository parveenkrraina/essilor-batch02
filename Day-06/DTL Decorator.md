# Sales Data Processing Pipeline

## Overview

- **Read** raw sales data from a CSV file.
- **Create** a bronze table for raw data (materialized table).
- **Create** a silver table with cleaned and filtered sales data (materialized table).
- **Use views** for intermediate transformations.
- **Add data quality checks** using `@dlt.expect` and `@dlt.expect_or_drop`.
- **Apply table properties** like `quality` and `autoOptimize`.

---

## Sample DLT Pipeline Code

```python
import dlt
from pyspark.sql.functions import col

# Bronze table: raw sales data loaded from CSV
@dlt.table(
    comment="Raw sales data loaded from CSV",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_sales():
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/FileStore/sales.csv")
    return df

# Silver view: clean data with some transformations
@dlt.view(
    comment="Intermediate cleaned sales data view"
)
def silver_sales_view():
    df = dlt.read("bronze_sales")
    return df.filter(col("Quantity") > 0)

# Silver table: materialized cleaned sales data with quality checks
@dlt.table(
    comment="Cleaned sales data with quality checks",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_quantity", "Quantity > 0")  # Drop invalid quantity rows
@dlt.expect("non_null_orderdate", "OrderDate IS NOT NULL")  # Log if order date is null
def silver_sales():
    return dlt.read("silver_sales_view")

# Optional: Streaming read example
# @dlt.table(comment="Streaming sales data")
# def streaming_sales():
#     return dlt.read_stream("bronze_sales")
```

---

## Explanation

- **@dlt.table**: Marks the function as a materialized Delta Live Table (DLT). The result is persisted as a Delta table.
- **@dlt.view**: Creates a non-materialized view for intermediate transformations.
- **dlt.read()**: Reads from an existing DLT table or view.
- **Data quality decorators**:
  - `@dlt.expect_or_drop`: Drops records where the condition fails (e.g., `Quantity <= 0`).
  - `@dlt.expect`: Logs records where the condition fails but keeps them (e.g., `OrderDate IS NULL`).
- **Table properties**:
  - `"quality"`: Assigns the data quality tier (`bronze` or `silver`).
  - `"pipelines.autoOptimize.managed"`: Enables Delta Lake auto-optimization.
- **Auto-Optimization**: Automatically optimizes query performance and resource usage.
- **CSV reading**: Loads data from the uploaded CSV file.
- **Transformations**: Basic filtering is applied to clean the data.
- **Data quality checks**: Expectations are set to ensure data quality.
- **Materialization**: The final output is stored as a Delta table.
- **Streaming**: Support for streaming data ingestion and processing.
- **Performance**: Optimized for high throughput and low latency.
- **Scalability**: Designed to handle large volumes of data efficiently.
- **Simplicity**: Simplifies the development of data pipelines with declarative syntax.
- **Extensibility**: Easily integrates with other data processing tools and frameworks.
- **Maintainability**: Facilitates easier updates and changes to data pipelines.
- **Observability**: Provides built-in monitoring and logging for data pipelines.
- **Security**: Ensures data access controls and compliance requirements are met.
- **Collaboration**: Supports teamwork and collaboration among data engineers and analysts.
- **Documentation**: Encourages clear documentation of data pipelines and transformations.
- **Testing**: Simplifies the process of testing and validating data transformations.
- **Versioning**: Supports version control for data pipelines and their components.
- **Reproducibility**: Ensures that data pipelines can be easily reproduced and audited.
- **Modularity**: Encourages the development of modular and reusable pipeline components.
- **Configurability**: Allows for easy configuration and customization of pipeline behavior.