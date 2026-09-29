from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "txn_type",
        "account_id",
        F.when(F.col("merchant").isin(["Starbucks", "Grab"]), "priority").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("account_id").desc())
)
