from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([3, 4, 18]))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "customer_id",
        F.upper("status").alias("status_upper"),
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)
