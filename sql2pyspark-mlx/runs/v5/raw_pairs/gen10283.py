from pyspark.sql import functions as F

by_dimension = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .groupingSets(
        [["account_type", "merchant"], ["account_type"], []],
        F.col("a.account_type").alias("account_type"),
        F.col("src.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.amount")), 2).alias("avg_amount"),
    )
)

result = (
    by_dimension
    .select("merchant", "cnt")
    .orderBy(F.col("merchant").desc_nulls_last())
)
