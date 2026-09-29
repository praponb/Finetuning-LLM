from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select(F.lit("account").alias("source"), "status")
    .unionByName(
        df_subscriptions
        .select(F.lit("subscription").alias("source"), "status")
    )
    .distinct()
    .orderBy("source")
)
