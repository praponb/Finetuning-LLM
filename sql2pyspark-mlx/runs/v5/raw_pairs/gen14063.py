from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("amount_per_refunded").desc())

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("quarter", "paid_at").alias("quarter_start"), "order_id")
    .agg(
        F.avg(F.when(F.col("status") == "REFUNDED", F.col("amount"))).alias("refunded_avg_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("amount_per_refunded"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .withColumn("amount_per_refunded_rank", F.rank().over(w3))
)
