from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.percentile_approx("is_new_burst", 0.5).alias("is_new_burst_p50"),
        F.min("paid_at").alias("first_seen"),
        F.percentile_approx("is_new_burst", 0.95).alias("is_new_burst_p95"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("amount_per_paypal"),
    )
)
