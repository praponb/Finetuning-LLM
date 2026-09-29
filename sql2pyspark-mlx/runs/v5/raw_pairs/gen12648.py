from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .cube(
        F.col("a.account_type").alias("account_type"),
        F.col("a.status").alias("status"),
        F.col("e.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.amount")).alias("max_amount"),
        F.grouping_id().alias("level_id"),
    )
)
