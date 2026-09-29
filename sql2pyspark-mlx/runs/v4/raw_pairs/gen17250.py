from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .rollup(
        "order_id",
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.max("paid_at").alias("last_seen"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.avg(F.when(F.col("method") == "paypal", F.col("island_id"))).alias("paypal_avg_island_id"),
    )
    .orderBy(F.col("is_new_session_tier").desc_nulls_first())
)
