from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id").isin([3, 18, 24]) & (F.col("o.status") == "COMPLETED"))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.col("c.country"),
        F.col("c.age"),
    )
)
