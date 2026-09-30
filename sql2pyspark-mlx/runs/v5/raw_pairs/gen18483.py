from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.15), 2))
)

prev_events = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.count("*").alias("event_count"),
        F.percentile_approx("amount_scaled", [0.25, 0.5, 0.75]).alias("amount_scaled_quartiles"),
    )
)
