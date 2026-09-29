from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "order_id",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
        F.avg("burst_id").alias("avg_burst_id"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min_2"),
        F.min("paid_at").alias("started_at"),
    )
    .filter(F.col("avg_burst_id") > 0)
)
