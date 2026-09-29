from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("origin"), "status")
    .unionByName(
        df_payments
        .filter(F.col("method") == "card")
        .select(F.lit("payment").alias("origin"), "status")
    )
    .distinct()
    .orderBy(F.col("origin").desc())
)
