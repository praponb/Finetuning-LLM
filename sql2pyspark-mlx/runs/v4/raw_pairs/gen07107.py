from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-03-26 20:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.account_type"), F.col("p.opened_date"))
)
