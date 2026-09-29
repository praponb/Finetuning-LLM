from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .pivot("txn_type", ["FEE", "DEBIT"])
    .agg(F.sum("amount").alias("amount"), F.count(F.lit(1)).alias("n"))
)
