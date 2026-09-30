from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.price_range") > 37)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("ss.price_range"),
        F.col("ss.brand_values"),
    )
    .orderBy(F.col("city").desc())
)
