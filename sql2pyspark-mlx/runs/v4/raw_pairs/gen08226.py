from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 90) & F.col("monthly_fee").between(120, 200))
    .groupBy(
        F.when(F.col("status").isin(["ACTIVE", "EXPIRED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)
