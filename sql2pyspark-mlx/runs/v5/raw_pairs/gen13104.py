from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Amazon", "Lazada"]))
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
)

transactions_stats = (
    valid_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.countDistinct("days_since_txn_time").alias("unique_accounts"))
)

result = (
    transactions_stats
    .filter(F.col("unique_accounts") >= 8)
    .select("txn_cohort", "unique_accounts")
    .orderBy(F.col("unique_accounts").desc())
)
