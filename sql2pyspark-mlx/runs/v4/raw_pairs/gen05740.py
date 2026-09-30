from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.when(F.col("status").isin(["EXPIRED", "ACTIVE"]), "group_a").otherwise("other").alias("status_group"))
    .pivot("status", ["PAUSED", "ACTIVE", "EXPIRED"])
    .agg(F.avg("monthly_fee"))
)
