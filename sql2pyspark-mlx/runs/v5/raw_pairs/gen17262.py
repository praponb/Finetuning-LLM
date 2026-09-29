from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .rollup("plan", "status")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
)
