from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("c.city"),
    )
    .orderBy("status")
)
