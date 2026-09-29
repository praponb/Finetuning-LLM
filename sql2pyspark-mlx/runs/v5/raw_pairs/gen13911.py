from pyspark.sql import functions as F

rollup_totals = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.account_type").alias("account_type"),
        F.col("s.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
    )
)

result = (
    rollup_totals
    .select(
        "account_type",
        "txn_type",
        "rows_total",
        "total_amount",
        F.when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "CORE").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy(F.col("total_amount").desc_nulls_first())
)
