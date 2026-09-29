from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
    )
    .orderBy(F.col("price").desc())
)
