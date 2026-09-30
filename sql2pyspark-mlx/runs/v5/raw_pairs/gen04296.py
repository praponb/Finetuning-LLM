from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("age") <= 60).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("accounts.opened_date").between("2025-03-01", "2025-09-07"))
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
    )
)
