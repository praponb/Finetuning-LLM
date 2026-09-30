from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("island_id") >= 7, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.sum("is_new_session").alias("total_is_new_session"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("island_id"))).alias("promptpay_total_island_id"),
    )
    .orderBy(F.col("island_id_tier").desc())
)
