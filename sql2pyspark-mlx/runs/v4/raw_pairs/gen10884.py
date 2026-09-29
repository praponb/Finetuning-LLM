from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.category_list"),
        F.col("t.pct_home"),
    )
    .orderBy(F.col("pct_home").desc())
)
