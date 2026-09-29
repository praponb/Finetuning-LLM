from pyspark.sql import functions as F

dim_totals = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("dim"), F.col("e.account_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.status").alias("status"),
        F.col("dim.account_type").alias("account_type"),
        F.col("e.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.amount")).alias("total_amount"),
    )
)

result = (
    dim_totals
    .select("status", "account_type", "merchant", "total_amount")
    .orderBy("status", F.col("account_type").desc())
)
