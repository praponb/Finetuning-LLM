from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Grab", "Amazon", "Starbucks"])
    .agg(F.count("amount"))
)
