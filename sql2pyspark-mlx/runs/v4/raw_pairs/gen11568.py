from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.email").like("%@gmail.com") & (F.col("c.age") >= 47))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
    )
)
