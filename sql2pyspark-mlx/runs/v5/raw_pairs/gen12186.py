from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(F.lit("account").alias("source"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.year("start_date") >= 2024)
        .select(F.lit("subscription").alias("source"), "status")
    )
    .orderBy(F.col("status").desc())
)
