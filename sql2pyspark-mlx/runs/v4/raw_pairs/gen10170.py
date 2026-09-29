from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("price").between(740, 920))
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("ss.last_launch_date"),
        F.col("ss.brand_values"),
    )
)
