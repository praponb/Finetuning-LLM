from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id")
w2 = Window.partitionBy("account_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("txn_type", "account_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

matched_keys = (
    df_transactions
    .select("id")
    .exceptAll(
        df_transactions
        .filter(F.col("amount") <= 2800)
        .select("id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.customer_id") == 17)
    .select("a.*", F.col("p2.balance"), F.col("p2.status"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.balance").alias("account_balance"), F.col("p.account_type"))
)

with_windows = (
    with_dims
    .select(
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "balance",
        "status",
        "account_balance",
        "account_type",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("account_balance").over(w2).alias("account_balance_sum_7d"),
        F.sum("balance").over(w3).alias("balance_sum_7d"),
        F.avg("balance").over(w4).alias("balance_avg_30d"),
    )
)

result = (
    with_windows
    .select(
        "txn_time",
        "is_flagged",
        "balance",
        "status",
        "account_balance_sum_7d",
        "balance_sum_7d",
        "balance_avg_30d",
    )
    .orderBy(F.col("txn_time").desc_nulls_last())
)
