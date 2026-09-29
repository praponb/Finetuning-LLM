from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([9, 10, 11]))
    .select(
        "id",
        "end_date",
        "start_date",
        "customer_id",
        F.lower("status").alias("status_lower"),
    )
)
