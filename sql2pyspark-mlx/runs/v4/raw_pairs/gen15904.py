from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount_rounded").desc_nulls_first())

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-18 03:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0).alias("is_debit"),
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

matched_keys = (
    valid
    .select("merchant")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([6, 19, 27]))
        .select("merchant")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_debit",
        "merchant_filled",
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

filtered = (
    base
    .filter(F.col("is_debit") <= 20)
    .select(
        "id",
        "txn_type",
        "merchant_lower",
        "resolved_merchant",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled_2"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("amount_rounded").over(w).alias("txn_type_sum_amount_rounded"),
        F.max("amount_rounded").over(w).alias("txn_type_max_amount_rounded"),
        F.percent_rank().over(w2).alias("amount_rounded_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount_rounded",
        "merchant_filled_2",
        "txn_type_max_amount_rounded",
    )
    .orderBy(F.col("amount_rounded").desc_nulls_last())
)
