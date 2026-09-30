from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") <= "2023-12-20")
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
