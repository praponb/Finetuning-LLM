from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2023-12-01")
    .groupBy("status")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .orderBy("max_monthly_fee")
)
