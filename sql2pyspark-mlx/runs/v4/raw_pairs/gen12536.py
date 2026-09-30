from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("source"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("start_date").alias("event_date"),
        F.lit("subscription").alias("source"),
    )
    )
)
