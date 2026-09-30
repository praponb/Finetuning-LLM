from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum("price").alias("total_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.total_price") > 1800)
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("t.min_launch_date"),
        F.col("t.total_price"),
    )
)
