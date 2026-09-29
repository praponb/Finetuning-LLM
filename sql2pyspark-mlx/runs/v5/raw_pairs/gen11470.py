from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 2000) & F.col("txn_type").isin(["FEE", "CREDIT", "REFUND"]))
    .groupBy(
        F.when(F.col("merchant").isin(["Grab", "Shell"]), "tier_1").otherwise("other").alias("merchant_group"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy("stddev_amount")
)
