from pyspark.sql import functions as F

enriched = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.account_type"))
)

valid = (
    enriched
    .filter((F.col("txn_time") >= F.lit("2023-07-21").cast("date")) & F.col("account_type").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "account_type",
        F.when(F.col("txn_type").isin(["FEE", "CREDIT"]), "Priority").otherwise("Other").alias("txn_type_group"),
    )
)

base = (
    valid
    .filter((F.col("txn_time") > F.lit("2023-10-31 12:00:00").cast("timestamp")) & F.col("account_type").isNotNull() & (F.col("amount") <= 1900))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "account_type",
        F.col("amount").cast("int").alias("amount_int"),
        F.when(F.col("txn_type") == "FEE", 1).otherwise(0).alias("is_fee"),
    )
)

summary = (
    base
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .groupingSets(
        [["year_start", "txn_type"], ["year_start"]],
        "year_start",
        "txn_type",
    )
    .agg(
        F.min("txn_time").alias("started_at"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    summary
    .select(
        "year_start",
        "txn_type",
        "started_at",
        "num_rows",
        "gid",
        F.when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
    )
)
