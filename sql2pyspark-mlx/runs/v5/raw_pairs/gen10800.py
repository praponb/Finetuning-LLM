from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "JP")
    .select(F.lit("customer").alias("record_type"), "signup_date")
    .union(
        df_subscriptions
        .filter(F.col("monthly_fee").between(130, 220))
        .select(F.lit("subscription").alias("record_type"), "end_date")
    )
    .distinct()
)
