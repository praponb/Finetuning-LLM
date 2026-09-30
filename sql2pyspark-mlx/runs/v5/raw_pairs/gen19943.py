from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("record_type"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("record_type"))
    )
    .distinct()
)
