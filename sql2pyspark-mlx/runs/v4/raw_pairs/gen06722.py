from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("num_products"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.num_products") > 4)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.num_products"),
        F.col("t.last_launch_date"),
    )
    .orderBy("id")
)
