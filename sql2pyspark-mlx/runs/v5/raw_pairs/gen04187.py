from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.upper(F.col("s.supplier_name")).like("%ACI%"))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.category")))).alias("category_values"),
        F.min(F.col("p.launch_date")).alias("first_launch_date"),
    )
)
