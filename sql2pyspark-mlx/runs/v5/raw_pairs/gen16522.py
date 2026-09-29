from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id")
w2 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-3, -1)

metrics = (
    df_subscriptions
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("monthly_fee_avg_3_prior", F.avg("monthly_fee").over(w2))
)

result = (
    metrics
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.sum("monthly_fee_share").alias("total_monthly_fee_share"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
    )
    .filter(F.col("expired_rows") < 1000)
    .orderBy("expired_rows")
)
