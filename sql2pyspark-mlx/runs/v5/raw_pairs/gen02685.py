from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([14, 26, 36, 37]) & F.col("merchant").isin(["Grab", "Shell"]))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
