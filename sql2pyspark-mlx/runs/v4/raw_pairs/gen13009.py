from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["standard", "basic"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("stddev_monthly_fee") >= 40)
)
