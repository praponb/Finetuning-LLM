from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & ((F.col("account_id") == 6) | F.col("account_id").isin([1, 3, 20, 29])))
    .groupBy(
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("avg_amount") > 25)
    .orderBy(F.col("avg_amount").desc())
)
