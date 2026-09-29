from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
        .select("status", "account_type")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "customer_id", "account_type", "opened_date")
    .orderBy(F.col("account_type").desc())
)
