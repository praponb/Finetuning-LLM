from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("price") >= 950)
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("ss.total_cost"),
        F.col("ss.brand_list"),
    )
)
