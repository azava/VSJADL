# Very Simple JSON Analyzer for Data Lake

This is a very simple PySpark Job that reads JSON data, remove duplicates and save in parquet format partitioned by columns. It can be used as base for more complex jobs.

You can use it to move data from a "Transient Zone" to a "Raw Zone" in a DataLake

Running:

```bash
spark-submit main.py \
--input_path INPUT_PATH \
--output_path OUTPUT_PATH \
--order_column ORDER_COLUMN \
--partition_columns PARTITION_COLUMNS
```

#### Parameters

**INPUT_PATH**

Path of input JSON Data. If the enviroment are configured correctly, you can use a HDFS, AWS S3 or GCS path:

Examples:
> /app/spark/data/in

> hdfs://data/in

> s3://your-bucket-name/path

**OUTPUT_PATH**

Path of output Parquet Data. As the INPUT_PATH, can be a HDFS, AWS S3 or GCS path if the enviroment are configured right

Examples:
>/app/spark/data/out

>hdfs://data/out

>s3://your-bucket-name/path

**ORDER_COLUMN**

A Timestamp column used to remove duplicates by ordering.

Examples:
>timestamp

>ts_event

>eventStartTime

**PARTITION_COLUMNS**

Unique columns  used in parquet partition. Sparate by comma. Ex.: type,day,time

Examples:
>date

>event_type,processed_date

>date,time

#### Running with docker

If you want to execute this job in a container with PySpark, first build container:

```bash
docker build -t pyspark-docker .
```

Then run to start container:

```bash
docker run -v $(pwd)/data:/app/spark/data -it pyspark-docker
```

Where the folder "./data" in your host machine will be mapped to "/app/spark/data" folder in container.

You will access container terminal. To activate PySpark run:

```bash
source pyspark_env
```

Then run Job command:

```bash
spark-submit main.py \
--input_path INPUT_PATH \
--output_path OUTPUT_PATH \
--order_column ORDER_COLUMN \
--partition_columns PARTITION_COLUMNS
```