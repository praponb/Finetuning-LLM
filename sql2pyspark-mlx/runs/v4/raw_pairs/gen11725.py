from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "Bangkok")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.max(F.col("p.cost")).alias("max_cost"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
    )
)
