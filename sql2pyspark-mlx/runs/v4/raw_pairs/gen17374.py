from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3000, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
