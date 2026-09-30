from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.order_id").isin([6, 28, 35]))
    .select(
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
)
