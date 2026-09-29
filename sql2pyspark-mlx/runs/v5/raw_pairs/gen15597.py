from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "customer_id")
    .agg(F.min("start_date").alias("first_start_date"))
)
