from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
        "session_no",
    )
    .agg(
        F.min("paid_at").alias("first_seen"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("is_new_burst")).otherwise(0)).alias("success_is_new_burst"),
        F.max("paid_at").alias("last_seen"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_status").desc_nulls_last())
)
