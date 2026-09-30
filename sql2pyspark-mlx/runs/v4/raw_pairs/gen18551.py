from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "merchant").orderBy("days_since_txn_time", "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-22 14:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.length("merchant").alias("merchant_length"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

ranked = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Starbucks", "Lazada"])
    .agg(F.sum("merchant_length").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "txn_type",
        F.col("7-Eleven_total").alias("v_7_eleven_total"),
        F.col("7-Eleven_item_count").alias("v_7_eleven_item_count"),
        F.col("Starbucks_total").alias("starbucks_total"),
        F.col("Starbucks_item_count").alias("starbucks_item_count"),
        F.col("Lazada_total").alias("lazada_total"),
        F.col("Lazada_item_count").alias("lazada_item_count"),
    )
)
