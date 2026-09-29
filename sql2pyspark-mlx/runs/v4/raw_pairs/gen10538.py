from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer", "card"]))
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
