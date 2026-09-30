from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("s.order_id") == 11) & (F.col("o.order_date") < "2024-11-01"))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.col("c.city"),
    )
    .orderBy(F.col("store_id").desc())
)
