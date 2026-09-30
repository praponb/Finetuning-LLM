from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD"))
    .cube("activity_month", "is_new_burst_tier")
    .agg(
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
        F.min("txn_time").alias("first_txn_time"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.max("txn_time").alias("ended_at"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_rows"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("is_new_burst")).otherwise(0)).alias("refund_is_new_burst"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy("amazon_rows")
)
