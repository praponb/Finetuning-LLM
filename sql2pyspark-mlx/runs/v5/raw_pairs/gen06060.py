from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 17) & (F.col("txn_type") != "REFUND"))
    .groupBy("txn_type", "is_flagged", F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("avg_amount") > 2800)
    .orderBy(F.col("unique_accounts").desc())
)
