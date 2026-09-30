from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-11-11 18:00:00").cast("timestamp")) & F.hour("paid_at").between(12, 19))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("order_id")
    .agg(
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.min("is_new_burst").alias("min_is_new_burst"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
)
