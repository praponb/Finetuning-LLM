from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 7, 9]))
    .withColumn("monthly_fee_band", F.when(F.col("monthly_fee") >= 238, "High").when(F.col("monthly_fee") >= 104, "Medium").otherwise("Low"))
)

summary = (
    base
    .groupBy("plan")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    summary
    .select("plan", "avg_monthly_fee", "min_monthly_fee")
    .orderBy("plan")
)
