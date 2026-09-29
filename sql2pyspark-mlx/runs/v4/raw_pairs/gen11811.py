from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id") == 40)
        .select(
        F.col("end_date").alias("signup_date"),
        F.lit("subscription").alias("source"),
    )
    )
    .distinct()
)
