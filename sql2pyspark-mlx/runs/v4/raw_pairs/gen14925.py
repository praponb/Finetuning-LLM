from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("amount_tier", F.when(F.col("amount") >= 1790, "LARGE").otherwise("SMALL"))
    .rollup("amount_tier")
    .agg(
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("amount_per_refunded"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.grouping("amount_tier").alias("grouping_id"),
    )
    .orderBy(F.col("success_count").desc())
)
