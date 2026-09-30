from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.order_date") <= "2023-04-01") & F.col("s.region").isin(["West", "East", "Central"]))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("c.is_active"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc())
)
