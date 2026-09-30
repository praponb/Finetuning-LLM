from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("dim"), F.col("src.account_id") == F.col("dim.id"), "left")
    .cube(
        F.col("dim.account_type").alias("account_type"),
        F.col("dim.status").alias("status"),
        F.col("src.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.amount")).alias("total_amount"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("account_type").asc_nulls_last())
)
