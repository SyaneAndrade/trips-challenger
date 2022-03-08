from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import sys

csv_file= sys.argv[1]
table = sys.argv[2]
master = sys.argv[3]

def spark_session():
    return (SparkSession
        .builder
        .master(master)
        .getOrCreate()
    )

def read_csv(spark):
    return (
        spark.read
        .format("csv")
        .option("header", True)
        .load(csv_file)
    )

def create_id(df):
    return df.withColumn("id", monotonically_increasing_id())

def select_columns(df):
    return df.select("id", "region", "origin_coord", "destination_coord", "datetime", "datasource")

def write_table(df):

    (df.write.format("jdbc")
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
    df = read_csv(spark)
    df = create_id(df)
    df = select_columns(df)
    write_table(df)