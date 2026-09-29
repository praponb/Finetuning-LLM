from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("account_type").isin(["CREDIT", "SAVINGS"]), "Core").otherwise("Other").alias("account_type_group"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

result = (
    valid
    .select("id")
    .exceptAll(
        df_accounts
        .filter(F.col("opened_date") >= "2025-04-06")
        .select("id")
    )
)
