from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-04-05").cast("date")) & (~F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select("*", F.upper("account_type").alias("account_type_upper"))
)

kept_keys = (
    base
    .select("customer_id")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") > "2025-02-14")
        .select("customer_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("opened_date") > F.lit("2023-09-10").cast("date"))
    .select(
        "*",
        F.concat("status", F.lit("/"), "account_type").alias("status_account_type"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "account_type_upper",
        "status_account_type",
        "status_clean",
        F.col("balance").cast("int").alias("balance_int"),
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
    )
)

base_accounts = (
    valid
    .filter(F.year("opened_date") == 2024)
    .select(
        "customer_id",
        "opened_date",
        "status",
        "account_type_upper",
        "status_account_type",
        "status_clean",
        "balance_int",
        "account_type_prefix",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

stats = (
    base_accounts
    .groupBy("customer_id")
    .agg(
        F.avg(F.when(F.col("status") == "OPEN", F.col("balance_int"))).alias("open_avg_balance_int"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.max(F.when(F.col("status") == "OPEN", F.col("balance_int"))).alias("open_max_balance_int"),
        F.percentile_approx("balance_int", 0.95).alias("balance_int_p95"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.percentile_approx("balance_int", [0.25, 0.5, 0.75]).alias("balance_int_quartiles"),
    )
)

result = (
    stats
    .select(
        "customer_id",
        "open_avg_balance_int",
        "frozen_events",
        "balance_int_p95",
        "open_rows",
        "balance_int_quartiles",
    )
    .orderBy(F.col("open_rows").desc_nulls_first())
)
