from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan", "monthly_fee", "end_date", F.trim("status").alias("clean_status"))
)
