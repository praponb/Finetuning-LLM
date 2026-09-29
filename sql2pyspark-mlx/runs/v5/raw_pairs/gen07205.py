from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("txn_type")
w3 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type")
w5 = Window.partitionBy("account_id").orderBy(F.col("balance").desc_nulls_first())
w_grp = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_last())

with_windows = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share_2"),
        F.sum("amount").over(w3).alias("running_amount"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("account_id").isin([6, 21, 25]))
        .select("id", "account_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.opened_date"), F.col("p.status"))
)

joined = (
    enriched
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .select(
        "src.account_id",
        "src.is_flagged",
        "src.amount_share",
        "src.running_amount",
        "src.opened_date",
        F.col("b.balance"),
        F.col("b.account_type"),
    )
)

enriched_transactions = (
    joined
    .select(
        "account_id",
        "is_flagged",
        "running_amount",
        "account_type",
        F.round((F.col("running_amount") / F.sum("running_amount").over(w4)), 4).alias("running_amount_share"),
        F.rank().over(w5).alias("balance_rank"),
        F.dense_rank().over(w_grp).alias("balance_dense_rank"),
    )
)

with_dims = (
    enriched_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.account_id",
        "e.running_amount",
        "e.account_type",
        "e.balance_rank",
        "e.balance_dense_rank",
        F.col("a.account_type").alias("account_account_type"),
    )
)

result = (
    with_dims
    .select("account_type", "balance_rank", "account_account_type")
    .orderBy(F.col("account_account_type").desc())
)
