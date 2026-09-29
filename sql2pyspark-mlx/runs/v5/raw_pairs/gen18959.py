from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "REFUND"]))
    .select(
        "id",
        "txn_time",
        "txn_type",
        F.col("amount").cast("int").alias("amount_int"),
        F.trim("txn_type").alias("clean_txn_type"),
    )
    .orderBy(F.col("txn_type").desc(), "id")
    .limit(10)
)
