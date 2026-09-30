from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.when(F.col("account_type").rlike("^(CHECKING|LOAN)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("account_type_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
