---
lab:
    title: 'Optimize Data Pipelines for Better Performance in Azure Databricks'
---

# Optimize Data Pipelines for Better Performance in Azure Databricks

Optimizing data pipelines in Azure Databricks can significantly enhance performance and efficiency. Utilizing Auto Loader for incremental data ingestion, coupled with the storage layer of Delta Lake, ensures reliability and ACID transactions. Implementing salting can prevent data skew, while Z-order clustering optimizes file reads by collocating related information. Azure Databricks' auto-tuning capabilities and the cost-based optimizer can further enhance performance by adjusting settings based on workload requirements.

> **Note**: The Azure Databricks user interface is subject to continual improvement. The user interface may have changed since the instructions in this exercise were written.

## Provision an Azure Databricks workspace

## Create a notebook and ingest data

1. In the sidebar, use the **(+) New** link to create a **Notebook** and change the default notebook name (**Untitled Notebook *[date]***) to **Optimize Data Ingestion**. Then, in the **Connect** drop-down list, select your cluster if it is not already selected. If the cluster is not running, it may take a minute or so to start.

2. In the first cell of the notebook, enter the following code, which uses *shell* commands to download data files from GitHub into the file system used by your cluster.

     ```python
    import urllib.request

    url = "https://github.com/parveenkrraina/essilor-batch02/raw/refs/heads/main/Day-08/yellow_tripdata_2021-01.parquet"
    dbfs_path = "/dbfs/nyc_taxi_trips/yellow_tripdata_2021-01.parquet"

    # Download the file to DBFS
    dbutils.fs.cp(url, dbfs_path, recurse=True)
     ```

3. Under the output from the first cell, use the **+ Code** icon to add a new cell and run the following code in it to load the dataset into a dataframe:
   
     ```python
    # Load the dataset into a DataFrame
    df = spark.read.parquet("/nyc_taxi_trips/yellow_tripdata_2021-01.parquet")
    display(df)
     ```

4. Use the **&#9656; Run Cell** menu option at the left of the cell to run it. Then wait for the Spark job run by the code to complete.

## Optimize Data Ingestion with Auto Loader:

Optimizing data ingestion is crucial for handling large datasets efficiently. Auto Loader is designed to process new data files as they arrive in cloud storage, supporting various file formats and cloud storage services. 

Auto Loader provides a Structured Streaming source called `cloudFiles`. Given an input directory path on the cloud file storage, the `cloudFiles` source automatically processes new files as they arrive, with the option of also processing existing files in that directory. 

1. In a new cell, run the following code to create a stream based on the folder containing the sample data:

     ```python
     df = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "parquet")
             .option("cloudFiles.schemaLocation", "/stream_data/nyc_taxi_trips/schema")
             .load("/nyc_taxi_trips/"))
     df.writeStream.format("delta") \
         .option("checkpointLocation", "/stream_data/nyc_taxi_trips/checkpoints") \
         .option("mergeSchema", "true") \
         .start("/delta/nyc_taxi_trips")
     display(df)
     ```

2. In a new cell, run the following code to add a new parquet file to the stream:

     ```python
    import urllib.request

    url = "https://github.com/parveenkrraina/essilor-batch02/raw/refs/heads/main/Day-08/yellow_tripdata_2021-02_edited.parquet"
    dbfs_path = "/nyc_taxi_trips/yellow_tripdata_2021-02_edited.parquet"

    # Download the file to DBFS
    dbutils.fs.cp(url, dbfs_path, recurse=True)
     ```
   
    The new file has a new column, so the stream stops with an `UnknownFieldException` error. Before your stream throws this error, Auto Loader performs schema inference on the latest micro-batch of data and updates the schema location with the latest schema by merging new columns to the end of the schema. The data types of existing columns remain unchanged.

3. Run the streaming code cell again and verify that two new columns (**new_column** and *_rescued_data**) were added to the table. The **_rescued_data** column contains any data that isn’t parsed due to type mismatch, case mismatch or column missing from schema.

4. Select **Interrupt** to stop the data streaming.
   
    The streaming data is written in Delta tables. Delta Lake provides a set of enhancements over traditional Parquet files, including ACID transactions, schema evolution, time travel, and unifies streaming and batch data processing, making it a powerful solution for managing big data workloads.

## Optimize Data Transformation

Data skew is a significant challenge in distributed computing, particularly in big data processing with frameworks like Apache Spark. Salting is an effective technique to optimize data skew by adding a random component, or 'salt', to keys before partitioning. This process helps distribute data more evenly across partitions, leading to a more balanced workload and improved performance.

1. In a new cell, run the following code to break a large skewed partition into smaller partitions by appending a *salt* column with random integers:

     ```python
    from pyspark.sql.functions import lit, rand

    # Convert streaming DataFrame back to batch DataFrame
    df = spark.read.parquet("/nyc_taxi_trips/*.parquet")
     
    # Add a salt column
    df_salted = df.withColumn("salt", (rand() * 100).cast("int"))

    # Repartition based on the salted column
    df_salted.repartition("salt").write.format("delta").mode("overwrite").save("/delta/nyc_taxi_trips_salted")

    display(df_salted)
     ```   

## Optimize Storage

Delta Lake offers a suite of optimization commands that can significantly enhance the performance and management of data storage. The `optimize` command is designed to improve query speed by organizing data more efficiently through techniques like compaction and Z-Ordering.

Compaction consolidates smaller files into larger ones, which can be particularly beneficial for read queries. Z-Ordering involves arranging data points so that related information is stored close together, reducing the time it takes to access this data during queries.

1. In a new cell, run the following code to perform compaction to the Delta table:

     ```python
    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, "/delta/nyc_taxi_trips")
    delta_table.optimize().executeCompaction()
     ```

2. In a new cell, run the following code to perform Z-Order clustering:

     ```python
    delta_table.optimize().executeZOrderBy("tpep_pickup_datetime")
     ```

This technique will co-locate related information in the same set of files, improving query performance.

## Clean up

In Azure Databricks portal, on the **Compute** page, select your cluster and select **&#9632; Terminate** to shut it down.

If you've finished exploring Azure Databricks, you can delete the resources you've created to avoid unnecessary Azure costs and free up capacity in your subscription.
