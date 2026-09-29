from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Grab") & ((F.col("merchant") == "Amazon") | F.col("txn_type").isin(["FEE", "DEBIT"])))
    .groupBy("is_flagged")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("amazon_count").desc())
)
