from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") | (F.col("monthly_fee") <= 230))
    .select(
        "id",
        "status",
        "customer_id",
        "start_date",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
    .orderBy("start_date", F.col("id").desc())
)
