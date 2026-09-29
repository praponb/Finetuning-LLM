from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(40000, 49000))
    .groupBy("status", "account_type")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("balance_range") >= 70)
    .select("status", "account_type", "latest_opened_date", "balance_range")
    .orderBy(F.col("status").desc())
)
