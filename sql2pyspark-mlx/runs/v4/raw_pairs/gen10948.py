from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type") == "CREDIT"), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.when(F.col("account_type").rlike("^(SAVINGS|CREDIT)$"), "GROUP_B").when(F.col("account_type").rlike("^(CHECKING|LOAN)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy("id")
)
