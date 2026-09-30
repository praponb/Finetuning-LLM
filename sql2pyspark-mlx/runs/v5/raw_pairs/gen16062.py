from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "DEBIT"]) | (F.col("amount") <= 2200))
    .groupBy("is_flagged")
    .agg(F.avg("amount").alias("avg_amount"))
)
