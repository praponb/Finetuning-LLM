from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") & (F.col("account_id") == 37))
    .select(
        "id",
        "txn_time",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
