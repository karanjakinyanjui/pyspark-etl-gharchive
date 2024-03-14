## Introduction:

GitHub Archive provides a wealth of data capturing various activities on the GitHub platform, such as repository creation, issues opened, and pull requests made. In this blog post, we'll explore how to use PySpark, a powerful analytics engine for big data processing, to analyze GitHub Archive data. We'll walk through the process of setting up a PySpark environment, fetching data from the GitHub Archive, transforming it, and storing the results for further analysis. It's worth noting that for demonstration purposes, we'll be using the local filesystem instead of HDFS.

## Setting up the Environment:

Before diving into the code, ensure you have PySpark installed. You can set up a PySpark environment locally or on a cluster, depending on your requirements. Additionally, make sure to install the `dotenv` Python package to manage environment variables conveniently in a `.env` file.

```bash
python3 install pyspark dotenv
```

## Downloading the data

To prepare for our analysis, we need to set up directories for storing input and output data. We achieve this using the following Bash commands:

```bash
# Setting up directories for input and output data
mkdir -p github-data/input github-data/output

# Fetching data from GitHub Archive using Bash
for i in {0..10}
do
    wget https://data.gharchive.org/2024-01-01-$i.json.gz -P github-data/input/
done
```

In these commands, we create directories named `github-data/input` and `github-data/output` using `mkdir -p` to ensure that they are created recursively if they don't already exist. We then use a loop to download data from GitHub Archive for the date range "2024-01-01" to "2024-01-11". Each file is fetched using `wget` and saved into the `github-data/input/` directory. This setup provides us with the necessary data to proceed with our analysis.

## Processing Data with PySpark:

In the Python code, we first import SparkSession and date functions. We then initialize a SparkSession which serves as the entry point to PySpark, with the master defined by the environment we are working in (development or production).

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
import os
from dotenv import load_dotenv

load_dotenv()

def get_spark():
    if os.environ.get("ENV") == "dev":
        master = "local"
    elif os.environ.get("ENV") == "prod":
        master = "yarn"
    spark = SparkSession.builder.master(master).appName("SparkSQL").getOrCreate()
    return spark

spark = get_spark()
```

The `read_data` function loads the downloaded JSON files into a Spark DataFrame.

```python
def read_data(data_dir, file_pattern, file_format):
    return spark.read.format(file_format).load(data_dir + file_pattern)
```

We then add date-related columns to the DataFrame using PySpark's built-in functions.

```python
def add_date_columns(df):
    return (
        df.withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
    )
```

## Writing Processed Data:

After processing the data, we write it back to disk in the Parquet file format. Parquet is a columnar storage format optimized for analytics workloads. It also uses snappy compression by default to optimize for storage space.

```python
def write_data(df, data_dir, file_format):
    res = (
        df.coalesce(16)
        .write.partitionBy("year", "month", "day")
        .mode("append")
        .format(file_format)
        .save(data_dir)
    )
```

## Running the code

```python
if __name__ == "__main__":
    cwd = os.getcwd()

    input_data_dir = "file://" + cwd + "/github-data/input"
    file_pattern = "2024*"
    input_file_format = "json"
    df = read_data(input_data_dir, file_pattern, input_file_format)
    df = add_date_columns(df)

    output_data_dir = "file://" + cwd + "/github-data/output"
    output_file_format = "parquet"
    write_data(df, output_data_dir, output_file_format)
```

The code segment enclosed within the `if __name__ == "__main__":` conditional block orchestrates the GitHub Archive data processing pipeline. Initially, it sets up the input and output data directories, utilizing the current working directory. A file pattern is specified to filter relevant files from the input directory, which are then loaded into a Spark DataFrame using the `read_data()` function. Subsequently, date-related columns are added to the DataFrame via the `add_date_columns()` function. The processed data is then written to the output directory in Parquet format, ensuring efficient storage and enabling subsequent analysis.

## Conclusion:

In this blog post, we demonstrated how to leverage PySpark to load, transform, and save GitHub Archive data efficiently. By harnessing the power of PySpark's distributed processing capabilities, we can handle large-scale datasets with ease. The ability to scale seamlessly from local development environments to production clusters makes PySpark a versatile tool for data analytics tasks. With the provided code and guidance, you can start exploring GitHub Archive data and uncover valuable insights into the GitHub ecosystem. Happy analyzing!
