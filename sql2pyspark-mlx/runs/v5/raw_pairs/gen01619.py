from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 10, 12]))
    .groupBy("status", "plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
    )
)
