from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 2300)
    .select(
        "txn_type",
        F.when(F.col("txn_type") == "REFUND", 1).otherwise(0).alias("is_refund"),
    )
    .orderBy("is_refund")
)
