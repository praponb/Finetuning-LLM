from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("customer_id", "id")
    .intersect(
        df_accounts
        .filter(F.col("opened_date").between("2025-01-01", "2025-07-17"))
        .select("customer_id", "id")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"), "status")
    .agg(
        F.avg(F.when(F.col("account_type") == "SAVINGS", F.col("balance"))).alias("savings_avg_balance"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
    .orderBy(F.col("checking_rows").desc_nulls_last())
)
