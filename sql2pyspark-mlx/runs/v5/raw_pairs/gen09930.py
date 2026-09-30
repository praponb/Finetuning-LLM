from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
    )
)
