from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance_rounded").desc(), "id")
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("balance_avg_3").desc_nulls_last())
w_recent = Window.partitionBy("status")
w6 = Window.partitionBy("status").orderBy(F.col("last_known_balance").asc_nulls_last())
w7 = Window.partitionBy("status").orderBy(F.col("balance_avg_3_dense_rank").desc_nulls_first())

valid = (
    df_accounts
    .filter((F.col("balance") < 69000) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 2).alias("balance_rounded"),
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

scored = (
    valid
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.ntile(3).over(w).alias("balance_rounded_bucket"),
        F.last("balance", ignorenulls=True).over(w2).alias("last_known_balance"),
        F.avg("balance").over(w3).alias("balance_avg_3"),
    )
)

kept_keys = (
    scored
    .select("account_type")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("account_type")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") >= 1600), "left_anti")
)

scored_accounts = (
    not_flagged
    .select(
        "status",
        "last_known_balance",
        F.dense_rank().over(w4).alias("balance_avg_3_dense_rank"),
    )
)

cleaned = (
    scored_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.round("last_known_balance", 1).alias("last_known_balance_rounded"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("last_known_balance") >= 35000)
    .select(
        "status",
        "last_known_balance",
        "balance_avg_3_dense_rank",
        F.min("balance_avg_3_dense_rank").over(w_recent).alias("status_min_balance_avg_3_dense_rank"),
        F.min("last_known_balance_rounded").over(w_recent).alias("status_min_last_known_balance_rounded"),
        F.round((F.col("balance_avg_3_dense_rank") / F.sum("balance_avg_3_dense_rank").over(w_recent)), 4).alias("balance_avg_3_dense_rank_share"),
        F.percent_rank().over(w6).alias("last_known_balance_percent_rank"),
        F.percent_rank().over(w7).alias("balance_avg_3_dense_rank_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "last_known_balance",
        "balance_avg_3_dense_rank",
        "status_min_last_known_balance_rounded",
        "balance_avg_3_dense_rank_share",
        "balance_avg_3_dense_rank_percent_rank",
    )
    .orderBy(F.col("last_known_balance").desc_nulls_first())
)
