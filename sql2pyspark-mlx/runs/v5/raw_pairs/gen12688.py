from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .cube(F.col("d.status").alias("status"), F.col("src.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.amount")).alias("min_amount"),
    )
    .orderBy(F.col("merchant").desc_nulls_last())
)
