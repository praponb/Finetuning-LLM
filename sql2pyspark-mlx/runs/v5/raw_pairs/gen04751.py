from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4300, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
