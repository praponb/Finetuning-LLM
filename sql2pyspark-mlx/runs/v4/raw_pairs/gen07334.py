from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("num_status").desc())

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        "status",
        F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("session_no_per_failed"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") > 1)
    .withColumn("num_status_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("status").desc())
)
