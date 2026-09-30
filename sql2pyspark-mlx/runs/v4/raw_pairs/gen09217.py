from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([6, 18]) & F.col("customer_id").isin([12, 18, 28, 29]))
    .select(
        "id",
        "status",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)
