from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "account_id",
        F.when(F.col("txn_type").isin(["FEE", "REFUND"]), "tier_1").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.avg("amount").alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") >= 75)
)
