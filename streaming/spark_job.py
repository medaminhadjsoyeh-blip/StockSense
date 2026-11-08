#!/usr/bin/env python3
"""Pipeline streaming : Kafka -> controles et agregations -> MongoDB."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

import mongo_sink

QUANTITY_MAX = 500      # quantite maximale acceptee
PRICE_GAP_MAX = 0.50    # ecart maximal au prix de reference (50 %)
TOTAL_TOLERANCE = 0.01  # tolerance d'arrondi sur le montant

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
])

spark = (SparkSession.builder.appName("StockSenseStreaming")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

mongo_sink.init_stock()

reference = (spark.read.option("multiLine", True)
             .json("results/average_price.json")
             .withColumnRenamed("average_price", "ref_price"))

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "sales")
       .option("startingOffsets", "earliest")
       .load()
       .selectExpr("CAST(value AS STRING) AS raw"))

parsed = (raw
          .withColumn("data", F.from_json("raw", SCHEMA))
          .select("raw", "data.*")
          .withColumn("event_time",
                      F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")))

required = ["transaction_id", "product_id", "quantity",
            "unit_price", "total_amount", "event_time"]
is_valid = F.lit(True)
for column in required:
    is_valid = is_valid & F.col(column).isNotNull()

enriched = parsed.join(reference, on="product_id", how="left")

price = F.col("unit_price")
ref = F.col("ref_price")
reasons = F.array_compact(F.array(
    F.when(~is_valid, "message_invalide"),
    F.when(F.col("quantity") > QUANTITY_MAX, "quantite_elevee"),
    F.when(price <= 0, "prix_negatif"),
    F.when((price > 0) & (F.abs(price - ref) / ref > PRICE_GAP_MAX), "prix_eloigne"),
    F.when(F.abs(F.col("quantity") * price - F.col("total_amount"))
           > TOTAL_TOLERANCE, "montant_incoherent"),
))

alerts = (enriched
          .withColumn("rule", F.explode(reasons))
          .select("raw", "rule", "transaction_id", "product_id", "quantity",
                  "unit_price", "ref_price", "total_amount", "event_time"))

valid = enriched.filter(is_valid).withWatermark("event_time", "10 minutes")

duplicates = (valid
              .groupBy(F.window("event_time", "10 minutes"), "transaction_id")
              .agg(F.count("*").alias("occurrences"))
              .filter(F.col("occurrences") > 1)
              .select("transaction_id", "occurrences"))

clean = (valid
         .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
         .dropDuplicatesWithinWatermark(["transaction_id"]))


def windowed(*keys):
    columns = [F.col(key) for key in keys]
    return (clean
            .groupBy(F.window("event_time", "1 minute"), *columns)
            .agg(F.count("*").alias("sales_count"),
                 F.round(F.sum("total_amount"), 2).alias("revenue"),
                 F.sum("quantity").alias("quantity"))
            .select(F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    *columns, "sales_count", "revenue", "quantity"))




def sink(frame, name, writer, mode="update"):
    def write_batch(batch, _batch_id):
        writer([row.asDict() for row in batch.collect()])

    return (frame.writeStream
            .outputMode(mode)
            .foreachBatch(write_batch)
            .option("checkpointLocation", "checkpoints/" + name)
            .trigger(processingTime="10 seconds")
            .start())


sink(alerts, "alerts", mongo_sink.write_alerts, mode="append")
sink(duplicates, "duplicates", mongo_sink.write_duplicates)
sink(windowed(), "sales_windows", mongo_sink.write_sales_windows)
sink(windowed("product_id"), "product_windows", mongo_sink.write_product_windows)
spark.streams.awaitAnyTermination()
