from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "txn_type",
        "merchant",
        F.when(F.col("txn_type").isin(["DEBIT", "FEE"]), "Core").otherwise("Other").alias("txn_type_group"),
    )
)
