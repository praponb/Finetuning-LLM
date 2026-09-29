from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.country") == "JP")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sort_array(F.collect_set(F.col("p.category"))).alias("category_list"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
    )
)
