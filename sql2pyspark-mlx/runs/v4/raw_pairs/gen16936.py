from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.year("end_date") >= 2023)
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
