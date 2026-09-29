from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 60)
    .select("id", "end_date", "monthly_fee")
)
