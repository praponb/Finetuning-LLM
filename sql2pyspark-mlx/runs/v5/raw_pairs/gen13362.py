from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("amount_tier").orderBy(F.col("num_status").desc())

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("amount") >= 1390, "LARGE").otherwise("SMALL").alias("amount_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.min("prev_paid_at").alias("first_prev_paid_at"),
        F.countDistinct("status").alias("distinct_status"),
        F.max("paid_at").alias("last_seen"),
    )
    .withColumn("num_status_rank", F.rank().over(w3))
    .orderBy(F.col("distinct_order_ids").desc())
)
