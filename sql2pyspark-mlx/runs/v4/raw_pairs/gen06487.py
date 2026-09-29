from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-08-09").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
)

matched_keys = (
    base
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("account_type") != "SAVINGS")
        .select("status", "account_type")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("opened_date") > F.lit("2023-06-25").cast("date"))
    .select(
        "*",
        F.ceil("balance").alias("balance_whole"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_anti")
)

grouped = (
    not_flagged
    .groupBy(
        F.when(F.col("balance") >= 58000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance_whole", 0.5).alias("balance_whole_p50"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
)

result = (
    grouped
    .select("balance_tier", "checking_count", "num_account_types")
    .orderBy(F.col("checking_count").desc_nulls_first(), "num_account_types")
)
