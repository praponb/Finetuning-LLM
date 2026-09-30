from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("kind"), "status")
    .unionByName(df_subscriptions.select(F.lit("subscription").alias("kind"), "status"))
    .distinct()
)
