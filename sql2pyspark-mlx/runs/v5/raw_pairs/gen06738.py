from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("account_balance")
w2 = Window.partitionBy("txn_type").orderBy("account_balance", "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .select("id", "account_id", "txn_type", F.upper("txn_type").alias("txn_type_upper"))
)

enriched = (
    valid
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.account_type"))
)

enriched_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .filter(~F.col("p.account_type").isin(["SAVINGS", "CHECKING", "CREDIT"]))
    .select(
        "s.*",
        F.col("p.account_type").alias("account_account_type"),
        F.col("p.balance"),
        F.col("p.opened_date"),
    )
)

enriched_transactions_2 = (
    enriched_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.balance").alias("account_balance"),
    )
)

enriched_transactions_3 = (
    enriched_transactions_2
    .select(
        "id",
        "account_id",
        "account_type",
        "opened_date",
        "account_opened_date",
        F.rank().over(w).alias("account_balance_rank"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

stats = (
    enriched_transactions_3
    .groupBy(
        F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.max(F.when(F.col("account_type") == "CHECKING", F.col("event_seq"))).alias("checking_max_event_seq"),
    )
)

result = (
    stats
    .select(
        "event_seq_tier",
        "year_start",
        "num_account_types",
        "checking_max_event_seq",
    )
    .orderBy(F.col("event_seq_tier").desc_nulls_first())
)
