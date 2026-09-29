from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("kind"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") != "CANCELLED")
        .select(F.lit("subscription").alias("kind"), "status")
    )
)
