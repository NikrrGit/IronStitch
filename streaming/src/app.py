from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_remove, coalesce, col, concat_ws, lit, struct, to_json, when
from pyspark.sql.avro.functions import from_avro

from sinks.duckdb import write_to_duckdb


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

# basic validation + error reason for DLQ routing
with_error = parsed.withColumn(
    "error_reason",
    concat_ws(
        ";",
        array_remove(
            array(
                when(col("order_id").isNull(), lit("missing_order_id")),
                when(col("order_item_id").isNull(), lit("missing_order_item_id")),
                when(col("product_id").isNull(), lit("missing_product_id")),
                when(col("seller_id").isNull(), lit("missing_seller_id")),
                when(col("price").isNull(), lit("missing_price")),
                when(col("freight_value").isNull(), lit("missing_freight_value")),
            ),
            lit(None),
        ),
    ),
)

valid_rows = with_error.filter(col("error_reason") == "").drop("error_reason")
invalid_rows = with_error.filter(col("error_reason") != "")

# write valid rows to DuckDB
valid_query = (
    valid_rows.writeStream.foreachBatch(write_to_duckdb)
    .option("checkpointLocation", "data/checkpoints/orders_v1_duckdb")
    .start()
)

# route invalid rows to DLQ topic
dlq_payload = invalid_rows.select(
    coalesce(col("order_id"), col("key")).cast("string").alias("key"),
    to_json(struct(*[col(c) for c in invalid_rows.columns])).alias("value"),
)

dlq_query = (
    dlq_payload.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("topic", "orders.dlq")
    .option("checkpointLocation", "data/checkpoints/orders_v1_dlq")
    .start()
)

spark.streams.awaitAnyTermination()
