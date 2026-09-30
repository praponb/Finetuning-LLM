from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 12, 36, 37]) & ((F.col("amount") <= 1900) | ~F.col("txn_type").isin(["FEE", "CREDIT"])))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .orderBy("merchant", "id")
    .limit(1)
)
