from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 3400)
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
