from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split
import sys

table_name = sys.argv[1]
master = sys.argv[2]


def spark_session():
    return SparkSession.builder.master(master).getOrCreate()


def read_table(spark, table):
    return (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgres/postgres")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("dbtable", table)
        .load()
    )


def parse_coordinator(coordenate):
    return coordenate.replace("POINT(", "").replace(")", "")


udf_parse_coordinator = udf(lambda coord: parse_coordinator(coord))


def get_coordinates(df):
    df = df.withColumn(
        "origin_coord", udf_parse_coordinator(col("origin_coord"))
    ).withColumn("destination_coord", udf_parse_coordinator(col("destination_coord")))
    df = (
        df.withColumn("origin_coord_x", split(col("origin_coord"), " ").getItem(0))
        .withColumn("origin_coord_y", split(col("origin_coord"), " ").getItem(1))
        .withColumn(
            "destination_coord_x", split(col("destination_coord"), " ").getItem(0)
        )
        .withColumn(
            "destination_coord_y", split(col("destination_coord"), " ").getItem(1)
        )
    )
    return df


def get_id_source(df, df_source):
    df_source = df_source.withColumnRenamed("id", "id_source")
    df = df.join(df_source, df.datasource == df_source.source)
    return df


def get_id_region(df, df_region):
    df_region = df_region.withColumnRenamed("id", "id_region")
    df = df.join(df_region, df.region == df_region.region)
    return df


def select_columns(df):
    return df.select(
        "id",
        "id_region",
        "origin_coord_x",
        "origin_coord_y",
        "destination_coord_x",
        "destination_coord_y",
        "datetime",
        "id_source",
    )


def write_table_trips(df):
    (
        df.write.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgres/postgres")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )


if __name__ == "__main__":
    spark = spark_session()
    df_trips = read_table(spark, "trips_raw")
    df_regions = read_table(spark, "region")
    df_source = read_table(spark, "source")
    df_trips = get_coordinates(df_trips)
    df_trips = get_id_source(df_trips, df_source)
    df_trips = get_id_region(df_trips, df_regions)
    df_trips = select_columns(df_trips)
    df_trips.show()
    write_table_trips(df_trips)
