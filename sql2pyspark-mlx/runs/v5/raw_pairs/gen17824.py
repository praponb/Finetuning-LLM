from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3000, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("num_rows").desc_nulls_last())
)
