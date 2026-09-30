from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 18, 33, 40]) & ((F.col("txn_type") == "CREDIT") | F.hour("txn_time").between(10, 14)))
    .select(
        "id",
        "merchant",
        "txn_type",
        F.when(F.col("txn_type").isin(["REFUND", "DEBIT"]), "Tier 1").otherwise("Other").alias("txn_type_group"),
    )
)
