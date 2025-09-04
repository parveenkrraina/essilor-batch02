---
lab:
    title: 'Real-time data processing with Spark Structured Streaming and Delta Lake with Azure Databricks'
---

# Real-time data processing with Spark Structured Streaming and Delta Lake with Azure Databricks

Spark Structured Streaming allows you to process data in real-time with end-to-end fault tolerance. Delta Lake enhances this by providing a storage layer with ACID transactions, ensuring data integrity and consistency. You can ingest data from cloud storage into Delta Lake, and use Delta Live Tables to manage and optimize your streaming data pipelines.



> **Note**: The Azure Databricks user interface is subject to continual improvement. The user interface may have changed since the instructions in this exercise were written.

## Provision an Azure Databricks workspace

## Create a notebook and ingest data

You can create notebooks in your Azure Databricks workspace to run code written in a range of programming languages. In this exercise, you'll create a simple notebook that ingests data from a file and saves it in a folder in Databricks File System (DBFS).

1. View the Azure Databricks workspace portal and note that the sidebar on the left side contains icons for the various tasks you can perform.

1. In the sidebar, use the **(+) New** link to create a **Notebook**.
   
1. Change the default notebook name (**Untitled Notebook *[date]***) to **RealTimeIngestion**.

1. In the first cell of the notebook, enter the following code, which uses *shell* commands to download data files from GitHub into the file system used by your cluster.

     ```python
    import urllib.request

    url = "https://raw.githubusercontent.com/parveenkrraina/essilor-batch02/refs/heads/main/Day-08/devices1.json"
    dbfs_path = "/device_stream/ddevices1.json"

    # Download the file to DBFS
    dbutils.fs.cp(url, dbfs_path, recurse=True)
     ```

1. Use the **&#9656; Run Cell** menu option at the left of the cell to run it. Then wait for the Spark job run by the code to complete.

## Use delta tables for streaming data

Delta lake supports *streaming* data. Delta tables can be a *sink* or a *source* for data streams created using the Spark Structured Streaming API. In this example, you'll use a delta table as a sink for some streaming data in a simulated internet of things (IoT) scenario. The simulated device data is in JSON format, like this:

```json
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{"device":"Dev2","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
```

1. Under the output for the first cell, use the **+ Code** icon to add a new cell. Then run the following code in it to create a stream based on the folder containing the JSON device data:

    ```python
   from pyspark.sql.types import *
   from pyspark.sql.functions import *
   
   # Create a stream that reads data from the folder, using a JSON schema
   inputPath = '/device_stream/'
   jsonSchema = StructType([
   StructField("device", StringType(), False),
   StructField("status", StringType(), False)
   ])
   iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)
   print("Source stream created...")
    ```

1. Add a new code cell and use it to perpetually write the stream of data to a delta folder:

    ```python
   # Write the stream to a delta table
   delta_stream_table_path = '/delta/iotdevicedata'
   checkpointpath = '/delta/checkpoint'
   deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
   print("Streaming to delta sink...")
    ```

1. Add code to read the data, just like any other delta folder:

    ```python
   # Read the data in delta format into a dataframe
   df = spark.read.format("delta").load(delta_stream_table_path)
   display(df)
    ```

1. Add the following code to create a table based on the delta folder to which the streaming data is being written:

    ```python
   # create a catalog table based on the streaming sink
   spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '{0}'".format(delta_stream_table_path))
    ```

1. Use the following code to query the table:

    ```sql
   %sql
   SELECT * FROM IotDeviceData;
    ```

1. Run the following code to add some fresh device data to the stream:

    ```python
    import urllib.request

    url = "https://raw.githubusercontent.com/parveenkrraina/essilor-batch02/refs/heads/main/Day-08/devices2.json"
    dbfs_path = "/device_stream/devices2.json"

    # Download the file to DBFS
    dbutils.fs.cp(url, dbfs_path, recurse=True)
    ```

1. Re-run the following SQL query code to verify that the new data has been added to the stream and written to the delta folder:

    ```sql
   %sql
   SELECT * FROM IotDeviceData;
    ```

1. Run the following code to stop the stream:

    ```python
   deltastream.stop()
    ```

## Clean up

In Azure Databricks portal, on the **Compute** page, select your cluster and select **&#9632; Terminate** to shut it down.

If you've finished exploring Azure Databricks, you can delete the resources you've created to avoid unnecessary Azure costs and free up capacity in your subscription.
