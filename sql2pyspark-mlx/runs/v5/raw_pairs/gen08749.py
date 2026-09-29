from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w_hist = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.orderBy(F.col("amount_max_30d").asc_nulls_last())

remaining_keys = (
    df_transactions
    .select("txn_type", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("amount").between(1800, 2900))
        .select("txn_type", "merchant")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("amount") > 1100)
    .select(
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.avg("amount").over(w).alias("txn_type_avg_amount"),
        F.max("amount").over(w_hist).alias("amount_max_30d"),
    )
)

filtered = (
    scored
    .filter(~F.col("merchant").eqNullSafe(F.lit("Shell")))
    .select(
        "txn_type",
        "amount",
        "is_flagged",
        "txn_type_avg_amount",
        "amount_max_30d",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

with_windows = (
    filtered
    .select(
        "txn_type",
        "amount",
        "amount_max_30d",
        "is_flagged_flag",
        F.avg("is_flagged_flag").over(w).alias("txn_type_avg_is_flagged_flag"),
        F.count("is_flagged_flag").over(w).alias("txn_type_count_is_flagged_flag"),
    )
)

scored_transactions = (
    with_windows
    .select(
        "amount",
        "amount_max_30d",
        "txn_type_avg_is_flagged_flag",
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.cume_dist().over(w4).alias("amount_max_30d_cume_dist"),
    )
)

result = (
    scored_transactions
    .groupBy(
        F.when(F.col("amount_max_30d_cume_dist") >= 0.82, "PREMIUM").otherwise("STANDARD").alias("amount_max_30d_cume_dist_tier"),
    )
    .agg(
        F.max("txn_type_avg_is_flagged_flag").alias("max_txn_type_avg_is_flagged_flag"),
        F.percentile_approx("txn_type_avg_is_flagged_flag", [0.25, 0.5, 0.75]).alias("txn_type_avg_is_flagged_flag_quartiles"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_first())
)
