from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
import sys

table = sys.argv[1]
master = sys.argv[2]


def spark_session():
    return SparkSession.builder.master(master).getOrCreate()


def read_table_trips(spark):
    return (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgres/postgres")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("dbtable", "trips_raw")
        .load()
    )


def create_region_trips(df):
    return df.select(col("datasource").alias("source")).distinct()


def create_id(df):
    return df.withColumn("id", monotonically_increasing_id() + 1)


def select_columns(df):
    return df.select("id", "source")


def write_table_source(df):
    (
        df.write.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgres/postgres")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )


if __name__ == "__main__":
    spark = spark_session()
    df_trips = read_table_trips(spark)
    df_source = create_region_trips(df_trips)
    df_source = create_id(df_source)
    df_source = select_columns(df_source)
    write_table_source(df_source)
