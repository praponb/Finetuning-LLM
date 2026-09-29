from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("s.sqft") != 15380)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("c.email"),
        F.col("c.age"),
    )
)
