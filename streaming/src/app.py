from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro


spark = (
    SparkSession.builder.appName("IronStitchStreaming")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# read the kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "orders.v1")
    .option("startingOffsets", "earliest")
    .load()
)

# load the schema json and decode
schema_path = Path(__file__).resolve().parents[2] / "schemas" / "order_item.v1.avsc"
schema = schema_path.read_text()

decoded = df.select(
    col("key").cast("string").alias("key"),
    from_avro(col("value"), schema).alias("data"),
)

# flatten
parsed = decoded.select("key", "data.*")

# temporary sink for validation
query = (
    parsed.writeStream.format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "data/checkpoints/orders_v1_console")
    .start()
)
query.awaitTermination()
