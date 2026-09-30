from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2025-10-01") | F.col("customer_id").isin([4, 12, 16]))
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        F.date_sub("start_date", 48).alias("start_date_minus_48d"),
    )
)
