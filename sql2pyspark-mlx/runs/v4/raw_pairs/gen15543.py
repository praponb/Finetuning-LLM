from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([10, 36]) & ((F.col("txn_time") < "2023-03-01") | F.col("is_flagged")))
    .withColumn("merchant_length", F.length("merchant"))
)

summary = (
    valid_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = summary.select("txn_cohort", "avg_amount")
