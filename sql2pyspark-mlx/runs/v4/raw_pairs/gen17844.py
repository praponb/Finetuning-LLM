from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("transactions.merchant").isNull() & (F.col("accounts.opened_date") > "2024-12-01"))
    .select(
        F.col("transactions.is_flagged"),
        F.col("accounts.account_type"),
        F.col("customers.email"),
    )
)
