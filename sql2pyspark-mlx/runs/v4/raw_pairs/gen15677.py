from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("merchant")
w4 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy(F.col("amount_max_30d").asc_nulls_first())
w7 = Window.partitionBy("account_id").orderBy(F.col("amount_max_30d").desc(), "id")

with_windows = (
    df_transactions
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.max("amount").over(w2).alias("amount_max_30d"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.round((F.col("first_known_amount") / F.sum("first_known_amount").over(w3)), 4).alias("first_known_amount_share"),
        F.avg("first_known_amount").over(w4).alias("first_known_amount_avg_30d"),
        F.count("amount_max_30d").over(w5).alias("amount_max_30d_count_30d"),
        F.percent_rank().over(w6).alias("amount_max_30d_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

filtered = (
    scored
    .filter(F.col("event_seq") < 4)
    .select(
        "id",
        "txn_type",
        "merchant",
        "first_known_amount",
        "first_known_amount_share",
        "first_known_amount_avg_30d",
        "amount_max_30d_count_30d",
        "amount_max_30d_percent_rank",
        "event_seq",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

kept_keys = (
    filtered
    .select("merchant", "id")
    .exceptAll(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("merchant", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("amount_int").between(42, 67))
    .select(
        "id",
        "merchant",
        "first_known_amount",
        "first_known_amount_share",
        "first_known_amount_avg_30d",
        "amount_max_30d_count_30d",
        "amount_max_30d_percent_rank",
        "event_seq",
        "amount_int",
        F.round((F.col("amount_max_30d_count_30d") / F.col("event_seq")), 2).alias("amount_max_30d_count_30d_per_event_seq"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("first_known_amount_share") >= 0.76, "PREMIUM").otherwise("STANDARD").alias("first_known_amount_share_tier"),
        "merchant",
    )
    .agg(
        F.min("first_known_amount_avg_30d").alias("min_first_known_amount_avg_30d"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount_int"))).alias("shell_total_amount_int"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy(F.col("merchant").desc_nulls_last())
)
