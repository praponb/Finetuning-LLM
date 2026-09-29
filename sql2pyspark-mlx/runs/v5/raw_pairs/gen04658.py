from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.lit("2023-06-16").cast("date")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.round((F.col("amount") * 100), 2).alias("adjusted_amount"),
        F.hour("txn_time").alias("txn_time_hour"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("amount") >= 4300, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.min("txn_time").alias("first_seen"),
        F.round(F.avg("adjusted_amount"), 2).alias("avg_adjusted_amount"),
    )
    .orderBy(F.col("amount_tier").desc())
)
