from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#  Initialize Spark with UI enabled
spark = SparkSession.builder \
    .appName("CryptoStreamingPipeline") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .getOrCreate()

# Define Schema for incoming Crypto Trades
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

#  Create a Streaming DataFrame (Simulating a socket or Kafka)
# Replace 'rate' with 'kafka' for a real production environment
raw_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()

# Transform: Add crypto logic (Simulated prices)
crypto_data = raw_stream.select(
    lit("BTC-USD").alias("symbol"),
    (col("value") + 60000).alias("price"), # Simulating BTC price
    rand().alias("volume"),
    col("timestamp")
)

# Aggregate: Create 1-minute Candlesticks (OHLC)
# This is a core Data Engineering pattern
windowed_ohlc = crypto_data \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ).agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("total_volume")
    )

# Sink: Write to Parquet for an Analytical Data Lake
query = windowed_ohlc.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
