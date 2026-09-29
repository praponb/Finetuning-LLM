from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@gmail.com") & (F.col("o.order_date") < "2025-07-24"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("s.store_name"),
        F.col("c.age"),
        F.col("c.is_active"),
    )
)
