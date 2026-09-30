from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy("account_type", "status")
w5 = Window.partitionBy("account_type").orderBy(F.col("balance_rank_2").desc_nulls_last())

matched_keys = (
    df_accounts
    .select("account_type", "status")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("account_type", "status")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("balance_rank", F.rank().over(w))
    .withColumn("balance_rank_2", F.rank().over(w2))
)

metrics = (
    enriched
    .withColumn("balance_rank_2_share", F.round((F.col("balance_rank_2") / F.sum("balance_rank_2").over(w3)), 4))
    .withColumn("balance_rank_share", F.round((F.col("balance_rank") / F.sum("balance_rank").over(w4)), 4))
    .withColumn("balance_rank_2_percent_rank", F.percent_rank().over(w5))
)

kept = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

with_dims = (
    kept
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.segment"), F.col("d.country"))
)

result = (
    with_dims
    .select(
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_rank",
        "balance_rank_2",
        "balance_rank_share",
        "balance_rank_2_percent_rank",
        "segment",
    )
    .orderBy(F.col("balance_rank_2").desc_nulls_first())
)
