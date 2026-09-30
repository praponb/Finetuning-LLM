from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.account_type"), F.col("d.status"))
)

enriched_transactions = (
    enriched
    .filter(F.col("account_id").isin([6, 14, 17, 28]))
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.max("amount").over(w2).alias("amount_max_30d"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

enriched_transactions_2 = (
    enriched_transactions
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.opened_date"))
)

kept_keys = (
    enriched_transactions_2
    .select("id", "merchant")
    .intersectAll(
        df_transactions
        .filter(~F.col("merchant").isin(["Lazada", "Amazon", "Starbucks"]))
        .select("id", "merchant")
    )
)

kept_rows = (
    enriched_transactions_2
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.account_type").alias("account_account_type"))
)

joined_transactions = (
    joined
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.status").alias("account_status"),
        F.col("d.account_type").alias("account_account_type_2"),
    )
)

enriched_transactions_3 = (
    joined_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.account_id",
        "a.txn_type",
        "a.merchant",
        "a.is_flagged",
        "a.account_type",
        "a.amount_max_30d",
        "a.cumulative_amount",
        "a.opened_date",
        "a.account_account_type",
        "a.account_account_type_2",
        F.col("b.account_type").alias("account_account_type_3"),
        F.col("b.status").alias("account_status_2"),
        F.col("b.opened_date").alias("account_opened_date"),
    )
)

result = (
    enriched_transactions_3
    .groupBy(
        "account_account_type",
        F.date_trunc("MONTH", "account_opened_date").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("account_account_type_3")).alias("num_account_account_type_3s"),
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
        F.sum(F.when(F.col("account_status_2") == "CLOSED", F.col("amount_max_30d")).otherwise(0)).alias("closed_amount_max_30d"),
    )
)
