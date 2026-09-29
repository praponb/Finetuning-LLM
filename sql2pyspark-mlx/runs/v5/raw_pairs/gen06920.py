from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("account_id", "merchant")
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_time_hour").desc_nulls_last())

filtered = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Grab"))) & (F.col("txn_type") == "REFUND"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"),
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

matched_keys = (
    filtered
    .select("merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
        .select("merchant")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select("*", F.avg("txn_time_hour").over(w_grp).alias("txn_time_hour_avg_7d"))
)

with_windows = (
    enriched
    .withColumn("txn_time_hour_count_30d", F.count("txn_time_hour").over(w_running))
    .withColumn("account_id_min_txn_time_hour", F.min("txn_time_hour").over(w3))
    .withColumn("txn_time_hour_dense_rank", F.dense_rank().over(w4))
)

grouped = (
    with_windows
    .groupBy(F.date_trunc("week", "txn_time").alias("week_start"), "merchant")
    .agg(
        F.percentile_approx("txn_time_hour_count_30d", [0.25, 0.5, 0.75]).alias("txn_time_hour_count_30d_quartiles"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
)

result = grouped.select("week_start", "merchant", "duration_min")
