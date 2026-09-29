from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("status", "account_type")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.hour(F.col("x.txn_time")).between(9, 12)), "left_anti")
)

with_dims = (
    eligible
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.credit_limit"))
)

filtered = (
    with_dims
    .filter(F.col("status").isNotNull())
    .select(
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "credit_limit",
        F.length("account_type").alias("account_type_length"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

with_dims_accounts = (
    filtered
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") == "Bangkok")
    .select(
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.opened_date",
        "src.resolved_status",
        F.col("c.age"),
        F.col("c.name"),
    )
)

grouped = (
    with_dims_accounts
    .groupBy(
        F.when(F.col("age") >= 63, "LARGE").otherwise("SMALL").alias("age_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("balance_per_loan"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("balance_per_loan") >= 0)
)

result = (
    grouped
    .select(
        "age_tier",
        "week_start",
        "balance_per_loan",
        "balance_stddev",
        "account_type_list",
        "avg_balance",
    )
)
