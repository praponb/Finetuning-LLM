from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("p.stock_qty")).alias("stock_qty_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.brand")))).alias("brand_values"),
    )
    .orderBy(F.col("brand_values").desc())
)
