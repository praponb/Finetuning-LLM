from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("launch_date").between("2023-05-01", "2025-07-30"))
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("ss.category_list"),
        F.col("ss.latest_launch_date"),
    )
)
