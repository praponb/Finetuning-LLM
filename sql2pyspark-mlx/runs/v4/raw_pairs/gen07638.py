from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter((F.col("p.cost") > 480) & ~F.col("p.brand").isin(["Initech", "Acme", "Globex"]))
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.store_id"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.coalesce(F.col("o.discount"), F.lit(0)).alias("discount_filled"),
    )
    .orderBy(F.col("product_id").desc(), "quantity")
)
