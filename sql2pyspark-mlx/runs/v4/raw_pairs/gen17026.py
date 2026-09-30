from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("status")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id").isin([2, 17, 20]))
        .select("status")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "customer_id", "account_type", "opened_date")
