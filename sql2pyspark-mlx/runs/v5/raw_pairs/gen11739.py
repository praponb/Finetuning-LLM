from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CHECKING", "CREDIT"]))
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date_plus_months")), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.is_flagged")), "left_anti")
)

cleaned_accounts = (
    kept
    .filter(F.col("status") == "OPEN")
    .withColumn("balance_band", F.when(F.col("balance") >= 64000, "High").when(F.col("balance") >= 9000, "Medium").otherwise("Low"))
    .withColumn("opened_date_minus_55d", F.date_sub("opened_date", 55))
    .withColumn("status_clean", F.coalesce("status", F.lit("UNKNOWN")))
)

result = (
    cleaned_accounts
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "opened_date_plus_months",
        "opened_date_minus_55d",
        F.when(F.col("status").rlike("^(FROZEN|OPEN)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
