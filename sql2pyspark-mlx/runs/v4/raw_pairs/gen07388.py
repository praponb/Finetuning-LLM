from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-05-28 20:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("session_id_per_refunded"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
)
