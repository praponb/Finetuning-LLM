from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("end_date") > "2023-09-01") & F.month("end_date").isin([5, 8, 12]))
    .withColumn("status_length", F.length("status"))
)

summary = (
    filtered_subscriptions
    .groupBy("plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.round(F.avg("status_length"), 2).alias("avg_status_length"),
    )
)

result = (
    summary
    .filter(F.col("unique_statuss") >= 5)
    .orderBy(F.col("avg_status_length").desc(), "plan")
    .limit(20)
)
