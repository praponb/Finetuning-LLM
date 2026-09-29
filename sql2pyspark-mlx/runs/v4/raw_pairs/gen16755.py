from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Lazada") & ~F.col("is_flagged") & (F.col("txn_time") >= "2025-01-14"))
    .select(
        "id",
        "txn_type",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.floor("amount").alias("amount_whole"),
    )
    .orderBy("amount_whole")
)
