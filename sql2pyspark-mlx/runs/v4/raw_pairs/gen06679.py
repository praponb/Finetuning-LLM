from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-02-01")
    .select(
        "txn_type",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.when(F.col("merchant").isin(["7-Eleven", "Lazada"]), "Tier 1").otherwise("Other").alias("merchant_group"),
    )
)
