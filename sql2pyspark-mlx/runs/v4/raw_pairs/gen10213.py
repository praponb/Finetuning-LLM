from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 170) & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "customer_id",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)
