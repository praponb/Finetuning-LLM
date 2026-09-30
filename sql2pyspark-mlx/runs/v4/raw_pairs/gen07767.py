from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .cube(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.countDistinct("method").alias("distinct_methods"),
        F.avg(F.when(F.col("status") == "FAILED", F.col("amount"))).alias("failed_avg_amount"),
    )
    .filter(F.col("paypal_count") >= 1)
    .orderBy("week_start")
)
