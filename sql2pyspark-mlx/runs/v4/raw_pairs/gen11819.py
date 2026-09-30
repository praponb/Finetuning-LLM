from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Amazon", "Shell"]))
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("latest_txn_time"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
    .filter(F.col("unique_txn_types") > 2)
    .drop("unique_txn_types")
    .orderBy(F.col("latest_txn_time").desc(), "total_amount")
)
