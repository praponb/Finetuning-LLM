from pyspark.sql import functions as F

result = (
    df_payments
    .rollup(
        F.when(F.col("amount") >= 1640, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
    )
    .orderBy(F.col("amount_tier").desc_nulls_last())
)
