from pyspark.sql import functions as F

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status"), F.col("b.opened_date"))
)

result = (
    joined
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "opened_date",
        F.when(F.col("merchant").rlike("^(Shell|Starbucks)$"), "PRIORITY").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("merchant_zone"),
    )
)
