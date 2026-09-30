from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("origin"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("end_date").alias("signup_date"),
        F.lit("subscription").alias("origin"),
    )
    )
    .distinct()
)
