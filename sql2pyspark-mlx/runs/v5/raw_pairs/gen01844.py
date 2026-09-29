from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.product_id").isin([7, 11, 21, 37]) & (F.col("oi.unit_price") <= 160))
    .select(
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)
