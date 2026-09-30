from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]))
    .select(
        "id",
        "txn_type",
        F.round("amount", 0).alias("amount_rounded"),
        F.when(F.col("txn_type").isin(["DEBIT", "FEE"]), "Tier 1").otherwise("Other").alias("txn_type_group"),
    )
)
