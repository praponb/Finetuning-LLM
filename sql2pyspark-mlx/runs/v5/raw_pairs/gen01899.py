from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["CANCELLED", "ACTIVE", "EXPIRED"]))
        .select("status", F.lit("subscription").alias("source"))
    )
)
