from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-05-11").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

result = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.opened_date").between("2025-04-01", "2025-09-01"))
    .select(
        "e.id",
        "e.account_id",
        "e.txn_time",
        "e.is_flagged_label",
        F.col("d.account_type"),
    )
)
