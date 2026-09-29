from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Starbucks", "7-Eleven"])
    .agg(F.avg("amount"))
    .select(
        "txn_type",
        F.col("Lazada").alias("lazada"),
        F.col("Starbucks").alias("starbucks"),
        F.col("7-Eleven").alias("v_7_eleven"),
    )
)
