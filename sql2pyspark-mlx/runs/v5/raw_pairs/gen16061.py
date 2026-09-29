from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") <= 3100))
    .select(
        "id",
        "merchant",
        F.to_date("txn_time").alias("txn_time_date"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("is_flagged_flag").desc())
)
