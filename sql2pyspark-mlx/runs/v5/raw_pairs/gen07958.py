from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating") <= 3.6)
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.date_sub(F.col("ss.sale_date"), 14).alias("sale_date_minus_14d"),
    )
    .orderBy(F.col("store_id").desc(), "city")
)
