from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("end_date").alias("event_date"))
    .unionByName(
        df_customers
        .filter(F.col("city") == "Bangkok")
        .select(F.col("signup_date").alias("event_date"))
    )
)
