from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Lazada", "Grab", "Amazon"])
    .agg(F.max("amount"))
)
