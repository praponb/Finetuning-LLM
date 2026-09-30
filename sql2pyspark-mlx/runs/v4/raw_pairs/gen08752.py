from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy(F.col("account_balance").desc())
w2 = Window.partitionBy("merchant").orderBy(F.col("account_opened_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("account_id").orderBy("dedup_rank", "id")

with_dims = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.status"), F.col("p2.balance"), F.col("p2.opened_date"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.*",
        F.col("a.account_type"),
        F.col("a.opened_date").alias("account_opened_date"),
        F.col("a.balance").alias("account_balance"),
    )
)

with_dims_transactions = (
    enriched
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.opened_date").alias("account_opened_date_2"))
)

ranked = (
    with_dims_transactions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    deduped
    .withColumn("dedup_rank_2", F.row_number().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
)

with_dims_transactions_2 = (
    latest
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.opened_date").alias("account_opened_date_3"))
)

result = (
    with_dims_transactions_2
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "status",
        "balance",
        "opened_date",
    )
)
