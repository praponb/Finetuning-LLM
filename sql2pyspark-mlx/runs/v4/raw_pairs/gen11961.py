from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.orderBy(F.col("txn_time_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-02-07 16:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("Unknown")))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("OTHER")))
)

matched_keys = (
    cleaned
    .select("txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant") == "Amazon")
        .select("txn_type")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    reconciled
    .select("id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount") <= 3500)
        .select("id", "txn_type")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isin(["Shell", "Starbucks"]))
    .select(
        "*",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.substring("merchant", 1, 4).alias("merchant_prefix"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.max("amount").over(w2).alias("amount_max_7d"),
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share_2"),
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share_3"),
        F.sum("amount").over(w3).alias("amount_sum_30d"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.opened_date"))
)

cleaned_transactions = (
    with_dims
    .filter(~F.col("is_flagged"))
    .select(
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        "merchant_filled",
        "resolved_merchant",
        "amount_max_7d",
        "amount_share_2",
        "amount_share_3",
        "amount_sum_30d",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant_2"),
    )
)

result = (
    cleaned_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.percentile_approx("amount_max_7d", 0.5).alias("amount_max_7d_p50"),
    )
)
