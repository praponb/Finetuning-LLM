from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id") == 31)
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
