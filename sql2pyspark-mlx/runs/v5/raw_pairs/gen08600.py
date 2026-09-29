from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("account_type", "customer_id")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id") == 18)
        .select("account_type", "customer_id")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("customer_id", "balance", "opened_date", "status")
    .orderBy("balance")
)
