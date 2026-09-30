from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2025-02-28")
    .select("start_date", F.ceil("monthly_fee").alias("monthly_fee_whole"))
)
