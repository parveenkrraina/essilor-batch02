# Delta Live Tables (DLT) Decorators and Methods

## Table of Contents
- [Explanation of Each Decorator and Method](#explanation-of-each-decorator-and-method)
- [Full List of DLT Decorators & Methods with Detailed Explanation](#full-list-of-dlt-decorators--methods-with-detailed-explanation)
- [Table Properties Commonly Used in DLT Pipelines](#table-properties-commonly-used-in-dlt-pipelines)

---

| Decorator / Method                  | Usage Example                                                      | Description                                                                                                    |
|-------------------------------------|--------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `@dlt.table`                       | `@dlt.table(...)\n`                                   | Defines a materialized Delta Live Table. Data is stored persistently as a Delta table, supporting incremental refreshes and optimization. Metadata like comments or properties can be added. |
| `@dlt.view`                        | `@dlt.view(...)\n`                                    | Defines a non-materialized view for transformations. Useful for intermediate calculations or logic without persisting data physically. |
| `dlt.read(table_name)`              | `dlt.read("bronze_sales")\n`                         | Reads data from an existing Delta Live Table earlier in the pipeline and returns a Spark DataFrame for further transformations. |
| `dlt.read_stream(table_name)`       | `dlt.read_stream("bronze_sales")\n`                  | Reads streaming data from a live table as a streaming DataFrame — perfect for near-real-time processing.        |
| `@dlt.expect(name, condition)`      | `@dlt.expect("non_null_order_date", "orderdate IS NOT NULL")\n` | Adds a data quality expectation: rows failing this condition are logged but not dropped, enabling monitoring without data loss. |
| `@dlt.expect_or_drop(name, condition)` | `@dlt.expect_or_drop("valid_quantity", "quantity > 0")\n` | Drops records failing the condition, enforcing strict data quality. Use when invalid data should be excluded.   |
| `@dlt.expect_or_fail(name, condition)` | _See below_                                                       | Fails the pipeline immediately if any record violates the condition — use for critical constraints.             |
| `dlt.write_stream(df, name)`        | `dlt.write_stream(df, "table_name")\n`               | Writes streaming data into a Delta Live Table. Rarely used, mostly for advanced streaming scenarios.            |

---

## Full List of DLT Decorators & Methods with Detailed Explanation

| Decorator / Method                        | Explanation                                                                 | Example                                                                                  |
|--------------------------------------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| `@dlt.table`                              | Marks a function as a materialized table in the DLT pipeline. This table is persisted as a Delta table. | `@dlt.table(comment="My Table")`                                       |
| `@dlt.view`                               | Marks a function as a view, which is not materialized but can be referenced in other DLT tables or views. | `@dlt.view(comment="My View")`                                         |
| `dlt.read(table_name: str)`                | Reads from a live table created earlier in the pipeline. Returns a Spark DataFrame. | `dlt.read("bronze_sales")`                                             |
| `dlt.read_stream(table_name: str)`         | Reads streaming data from a live table as a streaming DataFrame for streaming ETL use cases. | `dlt.read_stream("bronze_sales")`                                      |
| `@dlt.expect(name, condition)`             | Adds a data quality expectation; rows that fail are recorded but not dropped. | `@dlt.expect("valid_quantity", "quantity > 0")`                        |
| `@dlt.expect_or_drop(name, condition)`     | Adds a quality expectation that drops rows violating the condition.           | `@dlt.expect_or_drop("valid_quantity", "quantity > 0")`                |
| `@dlt.expect_or_fail(name, condition)`     | Adds an expectation that causes the pipeline to fail if the condition is violated by any row. | `@dlt.expect_or_fail("unique_id", "id IS UNIQUE")`                     |
| `dlt.write_stream(df, name)`               | Writes a streaming DataFrame into a live table, mainly for advanced streaming scenarios. | `dlt.write_stream(my_streaming_df, "table_name")`                      |
| `dlt.create_pipeline(**kwargs)`            | API to create a DLT pipeline programmatically (outside notebooks).            | `dlt.create_pipeline(name="my_pipeline", storage="dbfs:/path")`        |
| `dlt.start_pipeline(pipeline_id)`          | Starts a deployed DLT pipeline programmatically.                             | `dlt.start_pipeline("pipeline_id")`                                    |
| `dlt.stop_pipeline(pipeline_id)`           | Stops a running pipeline.                                                    | `dlt.stop_pipeline("pipeline_id")`                                   |
| `dlt.get_pipeline(pipeline_id)`            | Gets pipeline metadata/status.                                               | `dlt.get_pipeline("pipeline_id")`                                      |
| `dlt.list_tables()`                        | Lists tables in the current pipeline.                                        | `dlt.list_tables()`                                                    |

---

## Table Properties Commonly Used in DLT Pipelines

| Property                         | Description                                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `quality`                        | Defines the table quality tier: `"bronze"`, `"silver"`, `"gold"` - used to categorize data freshness and quality layers. |
| `pipelines.autoOptimize.managed` | Enables Delta Auto Optimize to improve file layout and performance.                                                      |
| `pipelines.catalog`              | Specifies the catalog name (like a database namespace) where the table is created.                                       |
| `pipelines.schema`               | The schema (database) name for table creation.                                                                           |
| `pipelines.trigger`              | Streaming trigger mode — either `"continuous"` for near real-time or `"microBatch"` for micro-batching.                  |
| `pipelines.checkpoint.interval`  | Interval for streaming checkpointing to ensure fault tolerance.                                                          |
| `pipelines.deployment`           | Deployment mode such as `"development"` or `"production"` controlling behavior and monitoring.                           |
| `pipelines.minTriggerInterval`   | Minimum interval between streaming trigger executions to avoid overload.                                                 |