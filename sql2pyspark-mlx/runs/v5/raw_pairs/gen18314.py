from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("price").alias("max_price"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.max_price") > 970)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("pbs.max_price"),
        F.col("pbs.min_launch_date"),
    )
    .orderBy("city", "supplier_name")
)
