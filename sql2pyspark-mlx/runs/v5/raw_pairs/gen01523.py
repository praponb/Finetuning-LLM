from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("account_status").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w3 = Window.orderBy("account_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_status", "account_id").orderBy(F.col("balance").desc(), "id")
w5 = Window.partitionBy("status", "account_status").orderBy("opened_date", "id")

latest = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
    )
)

joined = (
    latest
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .filter(~F.col("p2.status").isin(["CLOSED", "FROZEN"]))
    .select(
        "a.id",
        "a.account_id",
        "a.is_flagged",
        F.col("p2.opened_date"),
        F.col("p2.status"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.status").alias("account_status"),
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.balance"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "account_id",
        "account_opened_date",
        "balance",
        "dedup_rank",
        F.last("balance", ignorenulls=True).over(w3).alias("last_known_balance"),
        F.row_number().over(w4).alias("seq_no"),
        F.lead("balance", 2).over(w5).alias("next_balance"),
    )
)

remaining_keys = (
    scored
    .select("id", "account_id")
    .exceptAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "account_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

enriched_transactions = (
    kept_rows
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.balance").alias("account_balance"))
)

result = (
    enriched_transactions
    .rollup(
        F.when(F.col("balance") >= 77000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        F.when(F.col("account_balance") >= 71000, "PREMIUM").otherwise("STANDARD").alias("account_balance_tier"),
        F.date_trunc("week", "account_opened_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.countDistinct("account_id").alias("distinct_account_ids_2"),
    )
    .orderBy(F.col("balance_tier").asc_nulls_first())
)
