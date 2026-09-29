from pyspark.sql import functions as F

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.status"))
)

result = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("txn_type_zone"),
    )
    .orderBy(F.col("txn_time").asc_nulls_last(), F.col("amount").desc())
)
