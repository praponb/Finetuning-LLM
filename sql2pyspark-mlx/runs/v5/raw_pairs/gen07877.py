from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2023-06-01") & F.col("is_flagged") & F.col("txn_type").isin(["FEE", "DEBIT"]))
    .groupBy("merchant")
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
