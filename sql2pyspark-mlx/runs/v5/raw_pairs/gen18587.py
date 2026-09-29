from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 24)
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
    .orderBy("customer_id", "pct_enterprise")
)
