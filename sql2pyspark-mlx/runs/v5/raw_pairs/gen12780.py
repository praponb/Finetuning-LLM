from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 8]) | ~F.col("is_flagged"))
    .withColumn("clean_merchant", F.trim("merchant"))
    .orderBy("txn_time", F.col("amount").desc())
)
