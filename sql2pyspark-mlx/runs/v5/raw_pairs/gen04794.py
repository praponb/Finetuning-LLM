from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "DEBIT"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.when(F.col("status").rlike("^(CLOSED|OPEN)$"), "PRIORITY").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy("status_zone")
)
