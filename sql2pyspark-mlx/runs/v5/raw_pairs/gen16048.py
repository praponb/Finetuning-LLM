from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("amount") <= 3700)
    .select(
        "*",
        F.hour("txn_time").alias("txn_time_hour"),
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.round(F.avg("txn_time_hour"), 2).alias("avg_txn_time_hour"),
    )
    .filter(F.col("avg_txn_time_hour") >= 0)
)
