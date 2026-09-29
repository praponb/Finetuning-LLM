from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(
        df_payments
        .filter(F.col("method") == "cash")
        .select("amount", F.lit("payment").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
