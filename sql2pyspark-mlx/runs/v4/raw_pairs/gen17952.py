from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_time") < "2023-05-01")
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

joined = (
    base
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.account_type"))
)

enriched = (
    joined
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.balance"),
        F.col("p.status"),
        F.col("p.account_type").alias("account_account_type"),
    )
)

base_transactions = (
    enriched
    .filter(F.col("txn_time") > F.lit("2023-12-12").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_flag",
        "account_type",
        "balance",
        "account_account_type",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.coalesce("account_account_type", F.lit("NONE")).alias("resolved_account_account_type"),
    )
)

joined_transactions = (
    base_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.*",
        F.col("a.opened_date"),
        F.col("a.account_type").alias("account_account_type_2"),
    )
)

result = (
    joined_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged_flag",
        "account_type",
        "account_account_type",
        "status_prefix",
        F.when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "GROUP_B").otherwise("EXTENDED").alias("account_type_zone"),
    )
)
