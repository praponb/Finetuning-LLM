from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Starbucks", "Shell", "Grab", "Amazon"])
    .agg(F.max("amount"))
    .orderBy("txn_type")
)
