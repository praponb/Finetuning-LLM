from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_customers
        .filter(F.col("country") == "UK")
        .select("signup_date", F.lit("customer").alias("source"))
    )
    .distinct()
)
