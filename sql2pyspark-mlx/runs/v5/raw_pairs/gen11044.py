from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "FEE"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
