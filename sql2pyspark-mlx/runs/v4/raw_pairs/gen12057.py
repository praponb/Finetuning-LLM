from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "end_date", "status", "monthly_fee")
    .orderBy("id")
)
