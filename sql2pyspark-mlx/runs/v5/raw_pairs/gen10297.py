from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", F.year("txn_time").alias("txn_year"))
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 6)
    .select("account_id", "txn_year", "amount_count")
)
