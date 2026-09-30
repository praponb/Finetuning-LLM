from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "account_id")
    .pivot("merchant", ["Shell", "7-Eleven"])
    .agg(F.avg("amount"))
    .select(
        "txn_type",
        "account_id",
        F.col("Shell").alias("shell"),
        F.col("7-Eleven").alias("v_7_eleven"),
    )
)
