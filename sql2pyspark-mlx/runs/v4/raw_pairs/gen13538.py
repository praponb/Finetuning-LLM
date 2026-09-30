from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.price") >= 830)
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.avg(F.col("p.stock_qty")).alias("avg_stock_qty"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.brand")))).alias("brand_values"),
    )
    .orderBy("avg_stock_qty")
)
