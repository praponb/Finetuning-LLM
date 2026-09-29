from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 210) | F.col("status").isin(["PAUSED", "ACTIVE"]))
    .groupBy("status")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") >= 36)
    .orderBy("status")
)
