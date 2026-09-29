from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-01-01")
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)
