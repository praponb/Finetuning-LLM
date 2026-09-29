from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 18]))
    .groupBy("status")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("*").alias("account_count"),
    )
    .filter(F.col("account_count") >= 7)
    .select("status", "balance_range", "account_count")
    .orderBy(F.col("account_count").desc())
)
