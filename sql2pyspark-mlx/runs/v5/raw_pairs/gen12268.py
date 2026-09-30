from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "7-Eleven", "Shell"]) & (F.col("amount") < 2500))
    .select(
        "txn_time",
        "amount",
        F.when(F.col("merchant") == "Lazada", 1).otherwise(0).alias("is_lazada"),
    )
)
