from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("a.opened_date") <= "2024-05-01") & (F.length(F.col("c.name")) < 12))
    .select(
        F.col("a.balance"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("city").desc(), "credit_limit")
)
