from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["status", "account_type", "merchant"], ["status"]],
        F.col("p.status").alias("status"),
        F.col("p.account_type").alias("account_type"),
        F.col("s.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("cnt").desc())
)
