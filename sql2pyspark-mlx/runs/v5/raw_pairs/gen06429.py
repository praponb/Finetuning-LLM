from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.col("amount").alias("value"), F.lit("payment").alias("record_type"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") >= "2024-12-01")
        .select(
        F.col("monthly_fee").alias("value"),
        F.lit("subscription").alias("record_type"),
    )
    )
)
