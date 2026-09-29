from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isin([30, 31, 36]))
        .select("status")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("status").rlike("^(CLOSED|OPEN)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("balance").desc())
)
