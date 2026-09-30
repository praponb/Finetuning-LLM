from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("account_id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "Starbucks")
        .select("account_id", "merchant")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.balance") <= 32000)
    .select("s.*", F.col("b.status"), F.col("b.opened_date"), F.col("b.account_type"))
)

base = (
    joined
    .filter(F.col("merchant").isNotNull() & (F.col("account_id") == 35))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "status",
        "opened_date",
        "account_type",
        F.floor("amount").alias("amount_whole"),
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.opened_date").alias("account_opened_date"),
        F.col("d.balance"),
        F.col("d.account_type").alias("account_account_type"),
    )
)

filtered = (
    with_dims
    .filter((~F.col("account_account_type").eqNullSafe(F.lit("CREDIT"))) & (F.col("account_opened_date") <= "2023-11-01"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "status",
        "amount_whole",
        "is_frozen",
        "account_opened_date",
        "balance",
        "account_account_type",
        F.length("txn_type").alias("txn_type_length"),
        F.round((F.col("is_frozen") / F.col("amount")), 2).alias("is_frozen_per_amount"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

base_transactions = (
    filtered
    .filter(F.col("account_account_type").isNotNull())
    .withColumn("txn_time_date", F.to_date("txn_time"))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

result = (
    base_transactions
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "amount_whole",
        "is_frozen",
        "balance",
        "txn_type_length",
        "is_frozen_per_amount",
        "status_clean",
    )
    .orderBy(F.col("is_frozen").desc_nulls_last(), "balance")
)
