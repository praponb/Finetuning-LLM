from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("a.balance") <= 61000)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("c.country"),
    )
    .orderBy("country", "customer_id", "id")
    .limit(10)
)
