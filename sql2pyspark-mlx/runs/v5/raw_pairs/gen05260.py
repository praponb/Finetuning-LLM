from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("txn_type").desc())
)
