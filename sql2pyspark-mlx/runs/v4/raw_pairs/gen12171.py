from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "premium"]) & (F.col("start_date") >= "2024-02-07"))
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM"))
)

agg = (
    src
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = agg.filter(F.col("basic_monthly_fee") < 55)
