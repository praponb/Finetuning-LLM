from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("dim"), F.col("s.account_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.status").alias("status"), F.col("s.merchant").alias("merchant"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.amount")).alias("total_amount"),
    )
)
