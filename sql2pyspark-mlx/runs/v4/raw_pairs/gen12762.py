from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("end_date", "id")

metrics = (
    df_subscriptions
    .withColumn("monthly_fee_avg_6", F.avg("monthly_fee").over(w))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1).over(w2))
)

result = (
    metrics
    .groupBy("customer_id", F.date_trunc("MONTH", "end_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_avg_6"))).alias("expired_total_monthly_fee_avg_6"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.round(F.sum("monthly_fee_avg_6") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_avg_6_per_paused"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
)
