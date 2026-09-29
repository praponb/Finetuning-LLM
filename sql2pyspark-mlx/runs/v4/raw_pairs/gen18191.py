from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("c.segment"),
    )
    .orderBy("customer_id", "id")
)
