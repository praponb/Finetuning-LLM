from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("accounts.balance") < 67000)
    .select(
        F.col("customers.city"),
        F.col("customers.age"),
        F.col("customers.is_active"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
    )
)
