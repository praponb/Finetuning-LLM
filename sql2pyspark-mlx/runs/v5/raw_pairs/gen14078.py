from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
    .orderBy("stddev_amount")
)
