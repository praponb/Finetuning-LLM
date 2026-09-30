from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 1100) | F.hour("txn_time").between(12, 17))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .orderBy("total_amount")
)
