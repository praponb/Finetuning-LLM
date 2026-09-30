from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("dim"), F.col("e.account_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.status").alias("status"), F.col("e.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.amount")).alias("total_amount"),
    )
)
