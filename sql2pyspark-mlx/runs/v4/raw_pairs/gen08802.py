from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-02 11:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & F.col("amount").between(1500, 2300))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
    )
)

remaining_keys = (
    filtered
    .select("txn_type", "merchant")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([7, 21, 39]))
        .select("txn_type", "merchant")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select("e.*", F.col("a.status"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.max("amount").over(w2).alias("amount_max_30d"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
    )
)

base = (
    with_windows
    .filter((F.col("txn_time") > F.lit("2024-04-25 04:00:00").cast("timestamp")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        "is_flagged_label",
        "amount_dense_rank",
        "amount_percent_rank",
        (F.col("amount_dense_rank") * F.col("amount_max_30d")).alias("amount_dense_rank_times_amount_max_30d"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

with_dims_transactions = (
    base
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.status"), F.col("a.balance"), F.col("a.account_type"))
)

result = (
    with_dims_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged_label",
        "amount_dense_rank",
        "amount_percent_rank",
        "amount_dense_rank_times_amount_max_30d",
        "resolved_status",
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("amount").desc())
)
