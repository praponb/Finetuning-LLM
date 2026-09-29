from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("t.brand_list"), F.col("t.last_launch_date"))
)
