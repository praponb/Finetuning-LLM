from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("p.price"),
        F.round((F.col("p.cost") * 1.15), 2).alias("adjusted_cost"),
    )
    .orderBy(F.col("discount").desc())
)
