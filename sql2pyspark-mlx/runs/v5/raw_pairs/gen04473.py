from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
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
    islands
    .groupBy(
        F.when(F.col("session_id") >= 6, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .orderBy("amount_stddev")
)
