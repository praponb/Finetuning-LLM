from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isNotNull())
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("s.sqft"),
        F.col("s.city"),
    )
)
