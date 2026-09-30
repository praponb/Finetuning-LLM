from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([12, 13, 28, 29]) | (F.col("txn_time") >= "2023-08-01"))
)

summary = (
    recent_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    summary
    .filter(F.col("min_amount") < 2100)
    .orderBy(F.col("min_amount").desc())
)
