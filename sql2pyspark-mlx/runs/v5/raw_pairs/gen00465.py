from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "method",
        "paid_at",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("burst_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.max("burst_id").alias("max_burst_id"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("burst_id")).otherwise(0)).alias("promptpay_burst_id"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.max("paid_at").alias("last_seen"),
    )
)
