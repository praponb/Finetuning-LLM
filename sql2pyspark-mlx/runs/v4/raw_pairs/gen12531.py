from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([19, 21, 33]) & F.col("merchant").isin(["Shell", "Amazon", "Lazada"]) & F.col("amount").between(2400, 3700))
    .groupBy("txn_type")
    .agg(F.count("amount").alias("amount_count"))
)
