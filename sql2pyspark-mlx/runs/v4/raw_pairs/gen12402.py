from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.quantity").between(5, 8))
    .select(
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("oi.product_id"),
        (F.col("oi.quantity") - F.col("oi.unit_price")).alias("line_total"),
    )
)
