from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Stark"]))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("suppliers.supplier_name"),
        F.col("t.min_launch_date"),
        F.col("t.category_values"),
    )
)
