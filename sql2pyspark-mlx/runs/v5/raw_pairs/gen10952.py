from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 170)
    .select("id", "start_date", F.lower("status").alias("status_lower"))
)
