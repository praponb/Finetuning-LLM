from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.lit("transaction").alias("source"))
    .union(
        df_subscriptions
        .filter(F.col("plan").isin(["standard", "premium"]))
        .select("monthly_fee", F.lit("subscription").alias("source"))
    )
    .distinct()
)
