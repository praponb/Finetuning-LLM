from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active") & (F.year(F.col("a.opened_date")) == 2023))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.city"),
    )
)
