from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .union(
        df_accounts
        .filter(F.col("opened_date") < "2025-05-31")
        .select("opened_date", F.lit("account").alias("source"))
    )
    .distinct()
)
