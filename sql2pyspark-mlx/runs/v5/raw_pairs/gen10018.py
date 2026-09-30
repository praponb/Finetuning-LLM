from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("amount") <= 3100)

txn_day_summary = (
    valid_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    txn_day_summary
    .filter(F.col("unique_txn_types") < 8)
    .select("txn_day", "unique_txn_types")
    .orderBy(F.col("txn_day").desc())
)
