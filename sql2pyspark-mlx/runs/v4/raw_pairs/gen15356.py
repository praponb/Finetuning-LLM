from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter((F.col("p.launch_date") > "2023-10-22") & F.col("p.cost").between(370, 650))
    .select(
        F.col("oi.unit_price"),
        F.col("o.shipping_fee"),
        F.col("p.launch_date"),
        F.col("p.price"),
    )
    .orderBy(F.col("price").desc(), F.col("launch_date").desc())
)
