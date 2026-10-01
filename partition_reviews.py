import os
import sys
import time

# Ensure JAVA_HOME and HADOOP_HOME are set for Windows
if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ["JAVA_HOME"]):
    os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"

if "HADOOP_HOME" not in os.environ or not os.path.exists(os.environ["HADOOP_HOME"]):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"

bin_paths = f"{os.environ['HADOOP_HOME']}\\bin;{os.environ['JAVA_HOME']}\\bin;"
if bin_paths not in os.environ.get("PATH", ""):
    os.environ["PATH"] = bin_paths + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

def main():
    input_file = r"D:\Yelp_Analysis\Raw_data\yelp_academic_dataset_review.json"
    output_dir = r"D:\Yelp_Analysis\split_reviews"
    num_partitions = 10

    print(f"Starting PySpark review partition job...")
    print(f"Input: {input_file}")
    print(f"Output: {output_dir}")
    print(f"Number of partitions: {num_partitions}")

    start_time = time.time()

    # Initialize SparkSession with multi-core and memory allocation
    spark = (
        SparkSession.builder
        .appName("YelpReviewsPartitioner")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.shuffle.partitions", str(num_partitions))
        .config("spark.sql.debug.maxToStringFields", "100")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\nReading raw review JSON file with PySpark...")
    df = spark.read.json(input_file)

    print("\nDataset Schema:")
    df.printSchema()

    print(f"\nRepartitioning data into {num_partitions} partitions...")
    repartitioned_df = df.repartition(num_partitions)

    print(f"Writing partitioned JSON files to: {output_dir} ...")
    repartitioned_df.write.mode("overwrite").json(output_dir)

    spark.stop()

    elapsed = time.time() - start_time
    print(f"\nPartitioning completed successfully in {elapsed:.2f} seconds ({elapsed / 60:.2f} minutes)!")

if __name__ == "__main__":
    main()
