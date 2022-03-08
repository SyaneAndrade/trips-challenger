from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import sys

table = sys.argv[1]
master = sys.argv[2]

def spark_session():
    return (SparkSession
        .builder
        .master(master)
        .getOrCreate()
    )

def read_table_trips(spark):
    return (spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver") 
        .option("url", "jdbc:postgresql://postgres/postgres")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("dbtable", "trips")
        .load()
        )

def create_region_trips(df):
    return df.select("region").distinct()

def create_id(df):
    return df.withColumn("id", monotonically_increasing_id())

def select_columns(df):
    return df.select("id", "region")
    
def write_table_regions(df):
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
    df_trips = read_table_trips(spark)
    df_regions = create_region_trips(df_trips)
    df_regions = create_id(df_regions)
    df_regions = select_columns(df_regions)
    write_table_regions(df_regions)





    