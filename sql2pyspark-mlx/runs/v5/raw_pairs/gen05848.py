from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Singapore", "Tokyo"]) | F.col("a.opened_date").between("2024-03-01", "2025-08-01"))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
)
