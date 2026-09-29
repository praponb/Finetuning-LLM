from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("account_id").isin([2, 21, 36]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("amount") >= 4200, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
        F.min("txn_time").alias("first_txn_time"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.round(F.sum("is_flagged_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("is_flagged_flag_per_debit"),
    )
)
