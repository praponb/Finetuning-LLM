from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-03-21")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "CREDIT"), "left_anti")
)
