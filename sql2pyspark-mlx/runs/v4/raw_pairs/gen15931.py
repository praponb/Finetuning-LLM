from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.ceil("amount").alias("amount_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("is_flagged_flag") >= 60, "LARGE").otherwise("SMALL").alias("is_flagged_flag_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("amount_whole", 0.9).alias("amount_whole_p90"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
    .orderBy("amount_whole_p90")
)
