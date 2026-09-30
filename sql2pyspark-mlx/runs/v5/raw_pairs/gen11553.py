from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["account_type", "merchant"], ["account_type"]],
        F.col("p.account_type").alias("account_type"),
        F.col("src.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.amount")).alias("min_amount"),
    )
    .orderBy(F.col("rows_total").desc_nulls_first())
)
