from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") | (F.col("txn_time") >= "2025-01-01"))
    .select(
        "id",
        "is_flagged",
        "amount",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
    )
    .orderBy(F.col("amount").desc(), "is_flagged_label")
)
