from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy(F.col("source").desc())
)
