from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.signup_date").between("2024-07-02", "2025-02-01"))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.name"),
        F.col("c.country"),
        F.col("c.segment"),
    )
    .orderBy("segment")
)
