from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 130)
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
)

subscriptions_stats = (
    base
    .groupBy("status", F.year("start_date").alias("start_year"))
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.stddev("monthly_fee_whole"), 2).alias("stddev_monthly_fee_whole"),
    )
)

result = (
    subscriptions_stats
    .orderBy("status", "start_year")
    .limit(3)
)
