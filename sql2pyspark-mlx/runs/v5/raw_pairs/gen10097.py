from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.account_type").alias("account_type"),
        F.col("d.status").alias("status"),
        F.col("s.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.amount")).alias("avg_amount"),
    )
)
