from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").desc(), "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("merchant", 1, 4).alias("merchant_prefix"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status"), F.col("d.account_type"), F.col("d.balance"))
)

matched_keys = (
    enriched
    .select("txn_type", "merchant")
    .exceptAll(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("txn_type", "merchant")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.status").alias("account_status"),
        F.col("e.status").alias("transaction_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.balance")).alias("max_balance"),
    )
)
