from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Shell", "7-Eleven", "Grab"])
    .agg(F.max("amount"))
)
