from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

def load_data():
    spark = SparkSession \
        .builder \
        .appName("NYC Taxi Load") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Read transformed data
    input_path = "gs://nyc-tlc-yellow-2024/transformed/"
    nyc_data_df = spark.read.parquet(input_path) \
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType())) \
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))


    # Write to BigQuery
    nyc_data_df.repartition(50) \
        .write.format("bigquery") \
        .option("table", "gcp-data-project-478906.nyc_taxi_2024_dataset.raw_trips") \
        .option("project", "gcp-data-project-478906") \
        .option("writeMethod", "direct") \
        .option("location", "US") \
        .mode("overwrite") \
        .save()


    print("✓ Load completed")
    spark.stop()