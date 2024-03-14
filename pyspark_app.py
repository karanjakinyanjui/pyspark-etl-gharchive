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


def read_data(data_dir, file_pattern, file_format):
    return spark.read.format(file_format).load(data_dir + file_pattern)


def add_date_columns(df):
    return (
        df.withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
    )


def write_data(df, data_dir, file_format):
    res = (
        df.coalesce(16)
        .write.partitionBy("year", "month", "day")
        .mode("append")
        .format(file_format)
        .save(data_dir)
    )


if __name__ == "__main__":
    cwd = os.getcwd()
    input_data_dir = "file://" + cwd + "/github-data/input"
    output_data_dir = "file://" + cwd + "/github-data/output"
    print(input_data_dir)
    file_pattern = "2015*"
    file_format = "json"
    df = read_data(input_data_dir, file_pattern, file_format)
    df = add_date_columns(df)
    output_file_format = "parquet"
    write_data(df, output_data_dir, output_file_format)
