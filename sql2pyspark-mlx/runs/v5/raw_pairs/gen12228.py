from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.col("accounts.status"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
        F.col("customers.name"),
        F.upper(F.col("accounts.status")).alias("status_upper"),
    )
)
