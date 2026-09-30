from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("customer_id") == 22) & (F.col("status").isin(["EXPIRED", "CANCELLED", "PAUSED"]) | (F.col("monthly_fee") >= 140)))
)

agg = (
    filtered_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("start_date").alias("last_start_date"),
    )
)

result = agg.orderBy("last_start_date")
