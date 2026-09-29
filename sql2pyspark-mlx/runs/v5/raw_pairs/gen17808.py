from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("status", "plan", F.last_day("end_date").alias("end_date_month_end"))
)
