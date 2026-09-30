from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("p.product_name"),
        F.col("p.cost"),
    )
    .orderBy("cost", F.col("unit_price").desc())
)
