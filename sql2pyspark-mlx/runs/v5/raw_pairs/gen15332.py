from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2025)
    .select("opened_date", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("start_date").alias("opened_date"),
        F.lit("subscription").alias("source"),
    )
    )
)
