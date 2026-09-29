from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active") & (F.col("a.opened_date") <= "2023-10-01"))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
)
