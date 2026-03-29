from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -----------------------------
# Spark Session with Delta
# -----------------------------

spark = (
    SparkSession.builder
    .appName("SilverLayerDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -----------------------------
# Paths
# -----------------------------

BRONZE_PATH = "s3a://enterprise-streaming-dev-bronze/bronze/enriched/"
SILVER_PATH = "s3a://enterprise-streaming-dev-bronze/silver/events_delta/"

print("Reading Bronze JSON data...")

df = spark.read.json(BRONZE_PATH)

# -----------------------------
# Data Cleaning
# -----------------------------

df = (
    df
    .dropDuplicates(["event_id"])
    .withColumn("amount", col("amount").cast("double"))
)

print("Writing Silver data in DELTA format...")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)

print("Silver Delta table created successfully.")

spark.stop()