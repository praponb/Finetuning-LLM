from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Singapore", "Bangkok"]) & (F.col("s.rating") >= 3.5))
    .select(
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.rating"),
        F.col("s.city"),
        F.round((F.col("ss.revenue") * 1.15), 2).alias("adjusted_revenue"),
    )
)
