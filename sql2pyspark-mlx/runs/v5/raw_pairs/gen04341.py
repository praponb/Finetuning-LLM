from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("P.brand_list"),
        F.col("P.cost_count"),
    )
    .orderBy("country")
)
