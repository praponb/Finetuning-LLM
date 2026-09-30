from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Amazon"))) & ~F.col("txn_type").isin(["REFUND", "FEE"]))
    .withColumn("amount_rounded", F.round("amount", 1))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    valid
    .groupBy("account_id")
    .pivot("txn_type", ["REFUND", "DEBIT", "CREDIT"])
    .agg(F.sum("amount"))
)
