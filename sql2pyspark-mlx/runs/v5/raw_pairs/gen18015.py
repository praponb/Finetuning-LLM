from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Shell"))) & (F.col("txn_time") > F.lit("2023-07-13 08:00:00").cast("timestamp")))
    .select(
        "id",
        "txn_time",
        "merchant",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.length("txn_type").alias("txn_type_length"),
    )
)

result = (
    valid
    .select("id", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "merchant")
    )
)
