from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(~F.col("product_name").like("Y%"))
    .groupBy("supplier_id")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.countDistinct("stock_qty").alias("unique_brands"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.stock_qty_count") > 3)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("ss.stock_qty_count"),
        F.col("ss.unique_brands"),
    )
    .orderBy(F.col("country").desc(), "id")
    .limit(10)
)
