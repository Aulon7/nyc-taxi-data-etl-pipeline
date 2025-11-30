from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

def transform_data():
    spark = SparkSession \
        .builder \
        .appName("NYC Taxi Transform") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    input_path = "gs://nyc-tlc-yellow-2024/staging/"
    df = spark.read.option("mergeSchema", "true").parquet(input_path)

    # Filter invalid data (minimal cleaning)
    df_clean = df.filter(
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0) &
        col("passenger_count").between(1, 6) &
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    )

    # Deduplicate before writing
    df_clean = df_clean.dropDuplicates(["tpep_pickup_datetime", "tpep_dropoff_datetime", "VendorID"])

    # Round numeric columns
    df_clean = df_clean.withColumn("fare_amount", round(col("fare_amount"), 2)) \
                       .withColumn("total_amount", round(col("total_amount"), 2)) \
                       .withColumn("trip_distance", round(col("trip_distance"), 2))

    # Write partitioned by month
    output_path = "gs://nyc-tlc-yellow-2024/transformed/"
    df_clean.repartition(12) \
            .write.mode("overwrite") \
            .parquet(output_path)

    print("✓ Transform completed")
    spark.stop()