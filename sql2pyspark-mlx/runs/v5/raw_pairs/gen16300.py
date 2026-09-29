from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        "customer_id",
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") >= 29)
    .orderBy("plan")
)
