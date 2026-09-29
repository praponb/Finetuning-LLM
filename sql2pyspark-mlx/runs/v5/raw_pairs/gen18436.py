from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 11]))
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Grab", "Shell", "Starbucks"])
    .agg(F.count("amount"))
)
