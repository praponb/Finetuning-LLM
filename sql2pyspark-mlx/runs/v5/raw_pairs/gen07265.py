from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]) | F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 3600)
    .orderBy(F.col("stddev_amount").desc(), "merchant")
    .limit(1)
)
