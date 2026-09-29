from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 230) & F.col("customer_id").isNull())
    .groupBy("status")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") < 160)
    .orderBy("status")
)
