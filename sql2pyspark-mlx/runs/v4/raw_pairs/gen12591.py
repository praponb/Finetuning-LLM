from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "is_flagged",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
    )
)
