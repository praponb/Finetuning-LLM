from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 130))
    .withColumn("status_lower", F.lower("status"))
)

summary = (
    src
    .groupBy("plan", "status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = summary
