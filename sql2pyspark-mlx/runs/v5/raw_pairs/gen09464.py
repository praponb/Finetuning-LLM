from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("c.country") == "UK")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
)
