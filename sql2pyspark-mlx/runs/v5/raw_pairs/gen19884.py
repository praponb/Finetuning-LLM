from pyspark.sql import functions as F

dim_totals = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("dim"), F.col("src.account_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.status").alias("status"),
        F.col("src.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.amount")).alias("total_amount"),
    )
)

result = (
    dim_totals
    .select("status", "merchant")
    .orderBy(F.col("merchant").asc_nulls_first(), "status")
)
