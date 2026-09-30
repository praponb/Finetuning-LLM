from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.supplier_name").like("%Lion%"))
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.city"),
        F.coalesce(F.col("oi.quantity"), F.lit(0)).alias("quantity_filled"),
    )
)
