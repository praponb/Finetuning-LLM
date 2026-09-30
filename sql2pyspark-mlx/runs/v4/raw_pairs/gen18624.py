from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.upper(F.col("c.email")).like("%MMA%"))
    .select(
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("c.country"),
        F.col("c.email"),
    )
)
