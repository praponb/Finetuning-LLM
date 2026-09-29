from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-20 21:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select("*", F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"))
)

result = (
    filtered
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "FEE"])
    .agg(F.avg("days_since_txn_time").alias("amount"))
    .select("merchant", F.col("REFUND").alias("refund"), F.col("FEE").alias("fee"))
)
