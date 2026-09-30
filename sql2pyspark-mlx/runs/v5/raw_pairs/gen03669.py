from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Amazon"])
    .agg(F.sum("amount"))
    .select(
        "txn_type",
        F.col("7-Eleven").alias("v_7_eleven"),
        F.col("Amazon").alias("amazon"),
    )
)
