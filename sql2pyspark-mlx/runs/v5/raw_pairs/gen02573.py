from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2024-04-01") & F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .groupBy("status")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)
