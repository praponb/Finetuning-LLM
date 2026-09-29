from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("launch_date").between("2023-08-01", "2023-12-01"))
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.stddev_cost") >= 30)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.stddev_cost"),
        F.col("ss.category_values"),
    )
    .orderBy("id")
)
