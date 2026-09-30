from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([13, 19, 22, 33]) & ((F.col("monthly_fee") >= 190) | ~F.col("status").isin(["EXPIRED", "PAUSED", "ACTIVE"])))
)

agg = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    agg
    .filter(F.col("stddev_monthly_fee") < 63)
    .orderBy(F.col("stddev_monthly_fee").desc())
)
