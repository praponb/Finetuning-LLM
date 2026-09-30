from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .pivot("txn_type", ["DEBIT", "CREDIT", "REFUND"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("n"))
)
