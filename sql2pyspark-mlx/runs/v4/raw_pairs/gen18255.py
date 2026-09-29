from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "enterprise", "standard"]) & (F.col("customer_id") == 30))
    .groupBy("plan")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
    .orderBy("min_start_date")
)
