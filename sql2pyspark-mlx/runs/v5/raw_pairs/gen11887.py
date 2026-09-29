from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_type_length", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("status").orderBy(F.col("txn_type_length").desc_nulls_last())
w5 = Window.partitionBy("status").orderBy(F.col("is_new_burst").desc_nulls_last(), "id")

valid = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Grab"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.length("txn_type").alias("txn_type_length"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

filtered = (
    sessionized
    .filter(F.col("account_id").isin([2, 12]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "txn_type_length",
        "is_flagged_flag",
        "merchant_filled",
        "is_new_burst",
        "island_id",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        (F.col("txn_type_length") * F.col("is_flagged_flag")).alias("txn_type_length_times_is_flagged_flag"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.txn_type_length",
        "src.is_flagged_flag",
        "src.merchant_filled",
        "src.is_new_burst",
        "src.island_id",
        F.col("d.account_type"),
        F.col("d.status"),
    )
)

filtered_transactions = (
    with_dims
    .filter((~F.col("account_type").eqNullSafe(F.lit("CHECKING"))) & F.col("account_type").isNotNull())
    .withColumn("is_new_burst_times_is_flagged_flag", (F.col("is_new_burst") * F.col("is_flagged_flag")))
)

metrics = (
    filtered_transactions
    .select(
        "*",
        F.avg("island_id").over(w3).alias("island_id_avg_3_prior"),
        F.cume_dist().over(w4).alias("txn_type_length_cume_dist"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "txn_type",
        "txn_type_length",
        "is_flagged_flag",
        "is_new_burst",
        "island_id",
        "is_new_burst_times_is_flagged_flag",
        "island_id_avg_3_prior",
    )
)

result = (
    deduped
    .groupBy("island_id")
    .agg(
        F.percentile_approx("txn_type_length", 0.5).alias("txn_type_length_p50"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("txn_type_length_p50") < 1000000)
)
