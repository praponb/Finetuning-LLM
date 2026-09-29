from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-08-01")
    .withColumn("txn_type_lower", F.lower("txn_type"))
)

result = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.account_type").alias("account_type"),
        F.col("p.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.amount")).alias("max_amount"),
    )
)
