from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.countDistinct("monthly_fee").alias("unique_customers"))
    .filter(F.col("unique_customers") < 8)
)
