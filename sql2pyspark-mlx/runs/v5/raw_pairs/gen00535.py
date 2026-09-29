from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("amount") >= 2800) & F.hour("txn_time").between(8, 12)) | F.col("account_id").isin([4, 6, 34]))
    .groupBy("merchant", "account_id", "txn_type")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
    )
)
