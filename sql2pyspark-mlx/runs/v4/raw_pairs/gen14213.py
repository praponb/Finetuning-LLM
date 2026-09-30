from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("merchant") == "Starbucks"))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label_2"),
    )
    .orderBy(F.col("amount").desc())
)
