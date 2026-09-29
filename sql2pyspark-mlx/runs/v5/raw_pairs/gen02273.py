from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant") == "Starbucks"), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([3, 16, 34])), "left_anti")
)

eligible_accounts = (
    eligible
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type").isin(["REFUND", "CREDIT", "DEBIT"])), "left_anti")
)

not_flagged_accounts = (
    eligible_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (~F.col("x.txn_type").isin(["CREDIT", "FEE"])), "left_anti")
)

grouped = (
    not_flagged_accounts
    .groupingSets(
        [["account_type", "customer_id"], ["account_type"]],
        "account_type",
        "customer_id",
    )
    .agg(
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select(
        "account_type",
        "customer_id",
        "num_account_types",
        "gid",
        F.when(F.col("account_type").rlike("^(LOAN|CHECKING)$"), "GROUP_A").otherwise("GROUP_B").alias("account_type_zone"),
    )
    .orderBy(F.col("account_type").desc())
)
