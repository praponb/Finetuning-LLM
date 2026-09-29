from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-27 11:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .select("id", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "account_id")
    )
)
