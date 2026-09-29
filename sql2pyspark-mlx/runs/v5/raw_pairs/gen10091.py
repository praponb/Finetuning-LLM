from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("dim"), F.col("s.account_id") == F.col("dim.id"), "left")
    .cube(
        F.col("dim.status").alias("status"),
        F.col("dim.account_type").alias("account_type"),
        F.col("s.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("merchant")
)
