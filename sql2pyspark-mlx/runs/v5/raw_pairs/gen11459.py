from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "right")
    .filter(F.col("accounts.account_type") == "LOAN")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("accounts.opened_date"),
        F.col("customers.name"),
    )
    .orderBy(F.col("customer_id").desc())
)
