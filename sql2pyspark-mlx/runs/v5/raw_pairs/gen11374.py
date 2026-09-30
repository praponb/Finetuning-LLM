from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & (F.col("opened_date").between("2023-03-01", "2025-08-01") | F.col("balance").between(25000, 30000)))
    .select(
        "id",
        "balance",
        "customer_id",
        "account_type",
        F.round("balance", 2).alias("balance_rounded"),
    )
)
