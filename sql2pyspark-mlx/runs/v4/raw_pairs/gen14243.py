from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.cost"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.round(F.col("p.cost"), 1).alias("cost_rounded"),
    )
    .orderBy(F.col("discount").desc())
)
