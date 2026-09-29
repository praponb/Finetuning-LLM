from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 3000) & F.col("account_id").isin([2, 32, 35]) & F.col("merchant").isin(["Shell", "Grab", "Starbucks"]))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(F.max("txn_time").alias("last_txn_time"))
)
