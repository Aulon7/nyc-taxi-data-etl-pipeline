from pyspark.sql import SparkSession

def extract_data():
    spark = SparkSession \
        .builder \
        .appName("NYC Taxi Extract") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    input_path = "gs://nyc-tlc-yellow-2024/raw/*.parquet"
    nyc_data_df = spark.read.option("mergeSchema", "true").parquet(input_path)

    nyc_data_df = nyc_data_df.dropna(subset=["trip_distance", "fare_amount", "total_amount"])
    output_path = "gs://nyc-tlc-yellow-2024/staging/"
    nyc_data_df.repartition(12) \
                .write.mode("overwrite") \
                .parquet(output_path)

    print("✓ Extract completed")
    spark.stop()