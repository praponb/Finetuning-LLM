from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isin(["web", "mobile", "partner"]) & F.col("p.brand").isin(["Initech", "Umbrella"]))
    .select(
        F.col("oi.unit_price"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("o.shipping_fee"),
        F.substring(F.col("o.status"), 1, 4).alias("status_prefix"),
    )
)
