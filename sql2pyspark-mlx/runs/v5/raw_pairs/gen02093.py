from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating") > 3.6)
    .groupBy(F.col("s.city"))
    .agg(F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"))
)
