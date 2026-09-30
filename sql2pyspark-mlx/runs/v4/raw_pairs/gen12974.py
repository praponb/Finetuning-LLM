from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 170) & (F.col("status") == "EXPIRED") & (F.col("monthly_fee") >= 150))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 218, "high").when(F.col("monthly_fee") >= 37, "mid").otherwise("low").alias("monthly_fee_bucket"),
        F.year("start_date").alias("start_year"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("pct_paused") > 77)
    .select("monthly_fee_bucket", "start_year", "pct_paused")
)
