from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "plan", F.date_sub("start_date", 80).alias("start_date_minus_80d"))
)
