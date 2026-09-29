from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .select(
        "txn_type",
        "txn_time",
        F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0).alias("is_debit"),
    )
    .orderBy("txn_time", "txn_type")
)
