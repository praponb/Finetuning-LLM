from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-09").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    cleaned
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "DEBIT"])
    .agg(F.avg("is_flagged_flag"))
)
