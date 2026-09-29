from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("amount").alias("avg_amount"),
    )
)
