from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("S.brand_values"))
    .orderBy("brand_values")
)
