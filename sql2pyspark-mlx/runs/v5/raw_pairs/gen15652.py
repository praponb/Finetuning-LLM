from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("S.category_list"))
    .orderBy("id")
)
