from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.month("launch_date").isin([6, 7, 11]))
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.unique_suppliers") >= 7)
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("S.unique_suppliers"),
        F.col("S.category_list"),
    )
)
