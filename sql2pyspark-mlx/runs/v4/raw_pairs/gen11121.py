from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("customer_id") == 30) & F.month("end_date").isin([1, 2, 8])) | (F.col("start_date") <= "2024-09-01"))
    .select(
        "id",
        "monthly_fee",
        "customer_id",
        "plan",
        F.lower("plan").alias("plan_lower"),
    )
)
