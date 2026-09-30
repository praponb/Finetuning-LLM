from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.rating"), F.col("S.brand_list"))
    .orderBy("country", F.col("rating").desc())
)
