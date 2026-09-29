from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .select(F.col("end_date").alias("event_date"))
    .unionByName(
        df_customers
        .filter(F.col("segment") == "Consumer")
        .select(F.col("signup_date").alias("event_date"))
    )
    .distinct()
)
