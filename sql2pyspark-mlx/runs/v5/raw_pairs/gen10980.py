from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.lower(F.col("s.region")).alias("region_lower"),
    )
)
