from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "is_flagged",
        "txn_time",
        F.when(F.col("txn_type").isin(["CREDIT", "DEBIT"]), "Tier 1").otherwise("Other").alias("txn_type_group"),
    )
    .orderBy("txn_time", F.col("txn_type_group").desc())
)
