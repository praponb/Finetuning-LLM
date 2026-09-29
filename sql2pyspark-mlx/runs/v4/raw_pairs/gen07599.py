from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2025-05-01")
    .select("customer_id", "account_type")
    .unionByName(
        df_accounts
        .filter(F.col("account_type") != "SAVINGS")
        .select("customer_id", "account_type")
    )
    .distinct()
)
