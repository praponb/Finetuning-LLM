from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("c.city"),
    )
    .orderBy("credit_limit", "status")
)
