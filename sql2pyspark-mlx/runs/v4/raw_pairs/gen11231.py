from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 13) | F.col("amount").between(1200, 3300))
    .select(
        "merchant",
        "account_id",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("txn_time")
)
